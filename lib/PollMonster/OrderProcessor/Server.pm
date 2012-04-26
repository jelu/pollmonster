# PollMonster - a distributed data collection framework
# Copyright (C) 2010 Jerry Lundström
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PollMonster is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with PollMonster.  If not, see <http://www.gnu.org/licenses/>.

package PollMonster::OrderProcessor::Server;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Server;
use PollMonster::ProcessManager::Client;
use PollMonster::Processor;
use PollMonster::WorkDispatcher::Client;
use PollMonster::OrderProcessor::Dispatcher;
use PollMonster::OrderManager::Client;

use AnyEvent ();
use Coro ();
use Coro::Semaphore ();
use Coro::RWLock ();

use Log::Log4perl ();
use Scalar::Util qw(weaken);

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use PollMonster;

    my $foo = PollMonster->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=head2 function1

=cut

sub new {
    my $this = shift;
    my $class = ref($this) || $this;
    my %args = ( @_ );
    my $self = {
        logger => Log::Log4perl->get_logger(),
        registered => 0,
        order => {},
        processor => {},
        dispatcher => {},
        dispatcher_list => [],
        dispatcher_lock => Coro::RWLock->new,
        queue => [],
        queue_signal => Coro::Signal->new
    };
    bless $self, $class;
    my $rself = $self;
    weaken($self);

    if (exists $args{on_connect} and ref($args{on_connect}) eq 'CODE') {
        my $on_connect = $args{on_connect};
        $args{on_connect} = sub {
            shift;
            $on_connect->($self, @_);
        };
    }
    delete $args{on_error};
    delete $args{on_eof};
    delete $args{on_noauth};

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(add remove list result)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{add} = sub { $self->call_add(@_) };
    $args{service}->{remove} = sub { $self->call_remove(@_) };
    $args{service}->{list} = sub { $self->call_list(@_) };
    $args{service}->{result} = sub { $self->call_result(@_) };
    $args{service}->{stop} = sub { $self->call_stop(@_) };
    $args{service}->{start} = sub { $self->call_start(@_) };
    $args{service}->{restart} = sub { $self->call_restart(@_) };
    $args{service}->{load} = sub { $self->call_load(@_) };
    $args{service}->{reload} = sub { $self->call_reload(@_) };
    $args{service}->{unload} = sub { $self->call_unload(@_) };

    $args{name} = ORDER_PROCESSOR_NAME;
    $self->{rpc} = PollMonster::RPC::Server->new(%args);

    if ($self->{rpc}->is_alive) {
        my ($pt, $ps);
        $ps = Coro::Semaphore->new;
        $pt = AnyEvent->timer(
            after => PollMonster::PROCESS_LIST_INTERVAL,
            interval => PollMonster::PROCESS_LIST_INTERVAL,
            cb => sub {
                Coro::async_pool {
                    if (defined $self) {
                        my $g = $ps->guard;
                        $self->_dispatchers();
                    }
                    else {
                        $ps = undef;
                        $pt = undef;
                    }
                }
            });

        if (PollMonster::DEBUG) {
            my $qt; $qt = AnyEvent->timer(
                after => 5,
                interval => 5,
                cb => sub {
                    if (defined $self) {
                        $self->{logger}->debug('queue size ', scalar @{$self->{queue}});
                    }
                    else {
                        $qt = undef;
                    }
                });
        }

        my $coro = Coro::async {
            my ($err, $dispatcher_uri, $dispatcher, $payload);
            my $signal = $self->{queue_signal};

            while () {
                $signal->wait;
                last unless (defined $self);
                
                $self->{dispatcher_lock}->rdlock;
                while (@{$self->{queue}} and @{$self->{dispatcher_list}}) {
                    $dispatcher_uri = shift(@{$self->{dispatcher_list}});

                    unless (exists $self->{dispatcher}->{$dispatcher_uri}) {
                        $self->{logger}->warn('unable to find dispatcher uri ', $dispatcher_uri, ' in dispatchers');
                        next;
                    }

                    $dispatcher = $self->{dispatcher}->{$dispatcher_uri};

                    unless (defined $dispatcher and defined $dispatcher->rpc and $dispatcher->rpc->is_connected) {
                        $self->{logger}->warn('dispatcher invalid or disconnected');
                        next;
                    }

                    unless (defined $payload) {
                        $payload = shift(@{$self->{queue}});
                    }

                    $err = $dispatcher->rpc->queue($payload);
                    if (!defined $err) {
                        $self->{logger}->warn('unable to queue work to dispatcher ', $dispatcher->uri);
                        next;
                    }
                    elsif ($err == -1) {
                        next;
                    }

                    $payload = undef;
                    push(@{$self->{dispatcher_list}}, $dispatcher->uri);
                }

                if (defined $payload) {
                    unshift(@{$self->{queue}}, $payload);
                    $payload = undef;
                }
                $self->{dispatcher_lock}->unlock;
            }
        };
        $coro->desc('Queue '.$rself);
    }

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    my ($self) = @_;

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('destroy ', __PACKAGE__, ' ', $self);

    foreach (values %{$self->{processor}}) {
        $_->terminate;
    }

    delete $self->{processor};
    delete $self->{dispatcher};

    if ($self->is_registered and defined PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME)) {
        $self->unregister(PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME));
    }

    if (defined $self->{queue_signal}) {
        $self->{queue_signal}->send;
    }
}

=head2 function2

=cut

sub close {
    my ($self) = @_;

    if (defined $self->{rpc}) {
        foreach my $processor (values %{$self->{processor}}) {
            $processor->terminate;
        }
        $self->{processor} = {};

        $self->{dispatchers} = {};

        $self->{rpc}->close();
        $self->{rpc} = undef;

        $self->{order} = {};
    }

    $self;
}

=head2 function2

=cut

sub call_add {
    my ($self, $rpc, $cli, $uuid, $module, $parameter, $option) = @_;

    if (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'add',
                       errno => 1,
                       errstr => 'order exists'
                   ));
    }

    weaken($self);
    my $processor = PollMonster::Processor->new(
        uuid => $uuid,
        option => $option,
        module => $module,
        parameter => $parameter,
        on_work => sub { $self->processor_work(@_) },
        on_end => sub { $self->processor_end(@_) }
        );

    unless (defined $processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'add',
                       errno => 1,
                       errstr => 'unable to start processor'
                   ));
        return;
    }
           
    $self->{order}->{$uuid} = {
        uuid => $uuid,
        module => $module,
        parameter => $parameter,
        option => $option
    };

    $self->{processor}->{$uuid} = $processor->start;

    $self->{logger}->debug('order ', $uuid, ' added');

    $rpc->call($cli, 'add', 1);
    return;
}

=head2 function2

=cut

sub call_remove {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'remove',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
    }

    if (exists $self->{processor}->{$uuid}) {
        $self->{processor}->{$uuid}->terminate;
        delete $self->{processor}->{$uuid};
    }

    delete $self->{order}->{$uuid};

    $rpc->call($cli, 'remove', 1);
    return;
}

=head2 function2

=cut

sub call_list {
    my ($self, $rpc, $cli) = @_;

    $rpc->call($cli, 'list', values %{$self->{order}});
    return;
}

=head2 function2

=cut

sub call_stop {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }

    unless (exists $self->{processor}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'order not assigned'
                   ));
        return;
    }

    $self->{processor}->{$uuid}->stop;

    $rpc->call($cli, 'stop', 1);
    return;
}

=head2 function2

=cut

sub call_start {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }

    unless (exists $self->{processor}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'order not assigned'
                   ));
        return;
    }

    $self->{processor}->{$uuid}->start;

    $rpc->call($cli, 'start', 1);
    return;
}

=head2 function2

=cut

sub call_restart {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }

    unless (exists $self->{processor}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'order not assigned'
                   ));
        return;
    }

    $self->{processor}->{$uuid}->restart;

    $rpc->call($cli, 'restart', 1);
    return;
}

=head2 function2

=cut

sub call_load {
    my ($self, $rpc, $cli, $entry) = @_;

    PollMonster::ModuleFactory->instance->load($entry);

    $rpc->call($cli, 'load', 1);
    return;
}

=head2 function2

=cut

sub call_reload {
    my ($self, $rpc, $cli, $type, $module) = @_;

    if ($type eq 'processor') {
        unless (PollMonster::ModuleFactory->instance->have_processor($module)) {
            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'reload',
                           errno => 1,
                           errstr => 'module does not exists'
                       ));
            return;
        }

        PollMonster::ModuleFactory->instance->reload_processor($module);

        foreach my $processor (values %{$self->{processor}}) {
            if ($processor->{module} eq $module) {
                $processor->reload;
            }
        }
    }
    elsif ($type eq 'worker') {
        unless (PollMonster::ModuleFactory->instance->have_worker($module)) {
            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'reload',
                           errno => 1,
                           errstr => 'module does not exists'
                       ));
            return;
        }

        PollMonster::ModuleFactory->instance->reload_worker($module);
    }
    else {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'reload',
                       errno => 1,
                       errstr => 'unknown type'
                   ));
    }

    $rpc->call($cli, 'reload', 1);
    return;
}

=head2 function2

=cut

sub call_unload {
    my ($self, $rpc, $cli, $type, $module) = @_;

    if ($type eq 'processor') {
        PollMonster::ModuleFactory->instance->unload_processor($module);
    }
    elsif ($type eq 'worker') {
        PollMonster::ModuleFactory->instance->unload_worker($module);
    }
    else {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'unload',
                       errno => 1,
                       errstr => 'unknown type'
                   ));
    }

    $rpc->call($cli, 'unload', 1);
    return;
}

=head2 function2

=cut

sub processor_work {
    my ($self, $processor, $payload) = @_;

    if (@{$self->{queue}} > 10000) {
        return;
    }

    push(@{$self->{queue}}, $payload);

    if (@{$self->{dispatcher_list}}) {
        $self->{queue_signal}->broadcast;
    }

    return 1;
}

=head2 function2

=cut

sub processor_end {
    my ($self, $processor) = @_;
    my ($pm, $sig, @list, $ok);

    delete $self->{processor}->{$processor->uuid};
    delete $self->{order}->{$processor->uuid};

    $sig = Coro::Signal->new;
    $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            shift->list(ORDER_MANAGER_NAME);
        },
        service => {
            list => sub {
                shift;
                @list = @_;
                $sig->send;
            }
        }
        );
    $sig->wait;

    foreach my $om (@list) {
        my $cli = PollMonster::OrderManager::Client->new(
            uri => $om->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->ended($processor->uuid);
            },
            service => {
                ended => sub {
                    (undef, $ok) = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        last if ($ok);
    }

    return $ok ? 1 : 0;
}

=head2 function2

=cut

sub _dispatchers {
    my ($self) = @_;
    my (@o, @list, $timeout);

    unless ($self->is_registered and defined PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME)) {
        return;
    }

    unless (defined $self->{rpc}) {
        return;
    }

    {
        my $signal = Coro::Signal->new;
        push @o, AnyEvent->timer(
            after => 30,
            cb => sub { $timeout = 1; $signal->send; });
        push @o, PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub {
                $signal->send;
            },
            on_connect => sub {
                $_[0]->list(WORK_DISPATCHER_NAME);
            },
            service => {
                list => sub {
                    (undef, @list) = @_;
                    $signal->send;
                }
            });
        $signal->wait;
        @o = ();
    }

    if ($timeout) {
        $self->{logger}->warn('timeout retriving work dispatchers');
        return;
    }

    if (%{$self->{dispatcher}}) {
        my %dispatcher = map { $_->{uri} => $_ } @list;

        foreach my $dispatcher (values %{$self->{dispatcher}}) {
            unless (exists $dispatcher{$dispatcher->uri}) {
                delete $self->{dispatcher}->{$dispatcher->uri};

                $self->{logger}->info('lost dispatcher ', $dispatcher->uri);
            }
        }
    }

    foreach my $entry (@list) {
        unless (exists $self->{dispatcher}->{$entry->{uri}}) {
            my $wself = $self;
            weaken($wself);

            my ($signal, $error, $rpc);
            $timeout = 0;
            $signal = Coro::Signal->new;
            push @o, AnyEvent->timer(
                after => 30,
                cb => sub { $timeout = 1; $signal->send; });
            
            $rpc = PollMonster::WorkDispatcher::Client->new(
                uri => $entry->{uri},
                on_error => sub {
                    $wself->{logger}->warn('error from dispatcher ', $_[0]->uri, ': ', $_[1]);
                    delete $wself->{dispatcher}->{$_[0]->uri};

                    if (defined $signal) {
                        $error = $_[1];
                        $signal->send;
                    }
                },
                on_eof => sub {
                    delete $wself->{dispatcher}->{$_[0]->uri};
                },
                on_connect => sub {
                    if (defined $signal) {
                        $signal->send;
                    }
                },
                on_queue_ok => sub {
                    my ($rpc) = @_;

                    foreach (@{$wself->{dispatcher_list}}) {
                        if ($_ eq $rpc->uri) {
                            return;
                        }
                    }

                    push(@{$wself->{dispatcher_list}}, $rpc->uri);
                    $wself->{queue_signal}->broadcast;
                },
                service => {
                    result => sub {
                        my (undef, $uuid, $module, $payload, $private) = @_;

                        unless (exists $wself->{processor}->{$uuid}) {
                            return;
                        }

                        $wself->{processor}->{$uuid}->push_result($module, $payload, $private);
                        return;
                    }
                });
            $signal->wait;
            @o = ();
            $signal = undef;

            if ($timeout) {
                $self->{logger}->warn('timeout connecting to dispatcher ', $rpc->uri);
                next;
            }

            if (defined $error) {
                $self->{logger}->warn('error connecting to dispatcher ', $rpc->uri, ': ', $error);
                next;
            }

            my $dispatcher = PollMonster::OrderProcessor::Dispatcher->new(
                uri => $rpc->uri,
                rpc => $rpc
                );
            $self->{dispatcher}->{$dispatcher->uri} = $dispatcher;

            $self->{logger}->info('new dispatcher ', $dispatcher->uri);
        }
    }

    if (%{$self->{dispatcher}}) {
        $self->{dispatcher_lock}->wrlock;
        $self->{dispatcher_list} = [ map { $_->uri } values %{$self->{dispatcher}} ];
        $self->{dispatcher_lock}->unlock;
    }

    if (@{$self->{queue}} and @{$self->{dispatcher_list}}) {
        $self->{queue_signal}->broadcast;
    }

    return;
}

=head1 AUTHOR

Jerry Lundström, C<< <lundstrom.jerry at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-pollmonster at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=PollMonster>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc PollMonster


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=PollMonster>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/PollMonster>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/PollMonster>

=item * Search CPAN

L<http://search.cpan.org/dist/PollMonster/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2010 Jerry Lundström.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

PollMonster is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with PollMonster.  If not, see <http://www.gnu.org/licenses/>.


=cut

1;
