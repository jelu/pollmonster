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

package PollMonster::WorkDispatcher::Server;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Network;
use PollMonster::ProcessManager::Client;
use PollMonster::Worker::Client;
use PollMonster::WorkDispatcher::Worker;
use PollMonster::WorkDispatcher::Dispatcher;

use AnyEvent ();
use Coro ();
use Coro::Channel ();
use Coro::Signal ();
use Coro::Semaphore ();

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
        queue => [],
        queue_signal => Coro::Signal->new,
        queue_notify => {},
        dispatcher => {},
        worker => {},
        order_return => {},
        ready_list => []
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
    $args{on_net_join} = sub { $self->on_net_join(@_); };
    $args{on_net_leave} = sub { $self->on_net_leave(@_); };

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(queue sync-dispatchers)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{queue} = sub { $self->call_queue(@_) };
    $args{service}->{'sync-dispatchers'} = sub { $self->call_sync_dispatchers(@_) };

    $args{name} = WORK_DISPATCHER_NAME;

#            no_delay => 1,
#            autocork => 0

    $self->{rpc} = PollMonster::RPC::Network->new(%args);

    if ($self->{rpc}->is_alive) {
        $self->{dispatcher}->{$self->{rpc}->uri} = PollMonster::WorkDispatcher::Dispatcher->new(
            uri => $self->{rpc}->uri
            );

        my ($pt, $ps);
        $ps = Coro::Semaphore->new;
        $pt = AnyEvent->timer(
            after => PollMonster::PROCESS_LIST_INTERVAL,
            interval => PollMonster::PROCESS_LIST_INTERVAL,
            cb => sub {
                Coro::async_pool {
                    if (defined $self) {
                        my $g = $ps->guard;
                        $self->_loadbalance_dispatchers();
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
            my ($worker_uri, $worker, $payload, $err, $count);
            my $signal = $self->{queue_signal};

            while () {
                unless (@{$self->{queue}} and @{$self->{ready_list}}) {
                    $signal->wait;
                }
                last unless (defined $self);

                $count = 100;
                while (@{$self->{queue}} and @{$self->{ready_list}} and $count) {
                    $worker_uri = shift(@{$self->{ready_list}});

                    unless (exists $self->{worker}->{$worker_uri}) {
                        $self->{logger}->warn('unable to find worker uri ', $worker_uri, ' in workers');
                        next;
                    }

                    $worker = $self->{worker}->{$worker_uri};

                    unless (defined $worker and defined $worker->rpc and $worker->rpc->is_connected) {
                        $self->{logger}->warn('worker invalid or disconnected');
                        $self->_del_worker($worker);
                        next;
                    }

                    unless (defined $payload) {
                        $payload = shift(@{$self->{queue}});
                    }

                    $err = $worker->rpc->work(@$payload);
                    unless (defined $err) {
                        $self->{logger}->warn('unable to send work to worker ', $worker->uri);
                        $self->_del_worker($worker);
                        next;
                    }

                    $count--;
                    $payload = undef;
                }
                Coro::cede;

                if (defined $payload) {
                    unshift(@{$self->{queue}}, $payload);
                    $payload = undef;
                }

                $self->_queue_notify;
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

    delete $self->{worker};

    if ($self->is_registered and defined PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME)) {
        $self->unregister(PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME));
    }

    if (defined $self->{queue_signal}) {
        $self->{queue_signal}->send;
    }
}

=head2 function2

=cut

sub register {
    my ($self, $uri, $callback) = @_;

    unless (ref($callback) eq 'CODE') {
        $callback = undef;
    }

    return $self->SUPER::register(
        $uri,
        sub {
            if ($self->is_registered) {
                my $pm_cli; $pm_cli = PollMonster::ProcessManager::Client->new(
                    uri => $uri,
                    on_error => sub {
                        $pm_cli = undef
                    },
                    on_connect => sub {
                        my ($rpc, $name) = @_;
                        $rpc->list(WORK_DISPATCHER_NAME);
                    },
                    service => {
                        list => sub {
                            my ($rpc, @list) = @_;

                            Coro::async_pool {
                                foreach my $om (@list) {
                                    if (defined $self->{rpc}) {
                                        next if ($self->{rpc}->uri eq $om->{uri});
                                        last if ($self->{rpc}->is_joined);
                                        my $signal = Coro::Signal->new;
                                        $self->{rpc}->net_join($om->{uri}, sub { $signal->send });
                                        $signal->wait;
                                    }
                                }
                            };

                            $pm_cli = undef;
                        }
                    }
                    );
            }

            if (defined $callback) {
                $callback->($self);
            }
        }
        );
}

=head2 function2

=cut

sub on_net_join {
    my ($self, $rpc, $cli, $uri) = @_;

    unless (exists $self->{dispatcher}->{$uri}) {
        $self->{dispatcher}->{$uri} = PollMonster::WorkDispatcher::Dispatcher->new(
            uri => $uri
            );
    }

    return;
}

=head2 function2

=cut

sub on_net_leave {
    my ($self, $rpc, $cli, $uri) = @_;

    delete $self->{dispatcher}->{$uri};

    return;
}

=head2 function2

=cut

sub close {
    my ($self) = @_;

    if (defined $self->{rpc}) {
        $self->{workers} = {};

        $self->{rpc}->close();
        $self->{rpc} = undef;

        $self->{queue} = undef;
        $self->{order_return} = {};
    }

    $self;
}

=head2 function2

=cut

sub call_queue {
    my ($self, $rpc, $cli, $uuid, $module, $payload, $private) = @_;
    my ($worker_uri, $worker, $err);
    my $wcli = $cli;
    weaken($wcli);

    if (@{$self->{queue}} > 24000 and !exists $self->{queue_notify}->{$cli}) {
        $rpc->call($cli, 'queue_full');
        $self->{queue_notify}->{$cli} = $wcli;
        $self->{logger}->debug('queue full, notifying ', $cli->uri);
    }

    $self->{order_return}->{$uuid} = $wcli;

    while (($worker_uri = shift(@{$self->{ready_list}}))) {
        unless (exists $self->{worker}->{$worker_uri}) {
            $self->{logger}->warn('unable to find worker uri ', $worker_uri, ' in workers');
            next;
        }

        $worker = $self->{worker}->{$worker_uri};

        unless (defined $worker and defined $worker->rpc and $worker->rpc->is_connected) {
            $self->{logger}->warn('worker invalid or disconnected');
            $self->_del_worker($worker);
            next;
        }

        $err = $worker->rpc->work($uuid, $module, $payload, $private);
        unless (defined $err) {
            $self->{logger}->warn('unable to send work to worker ', $worker->uri);
            $self->_del_worker($worker);
            next;
        }

        return;
    }

    if (defined $private) {
        push(@{$self->{queue}}, [ $uuid, $module, $payload, $private ]);
    }
    else {
        push(@{$self->{queue}}, [ $uuid, $module, $payload ]);
    }
    $self->{queue_signal}->broadcast;

    return;
}

=head2 function2

=cut

sub _queue_notify {
    my ($self) = @_;

    if (defined $self->{rpc} and @{$self->{queue}} < 8000 and %{$self->{queue_notify}}) {
        foreach my $cli (keys %{$self->{queue_notify}}) {
            if (defined $self->{queue_notify}->{$cli}) {
                $self->{rpc}->call($self->{queue_notify}->{$cli}, 'queue_ok');
                $self->{logger}->debug('queue ok, notifying ', $self->{queue_notify}->{$cli}->uri);
            }
            delete $self->{queue_notify}->{$cli};
        }
    }

    return;
}

=head2 function2

=cut

sub result {
    my ($self, $uuid, $module, $payload, $private) = @_;

    unless (exists $self->{order_return}->{$uuid}) {
        return;
    }

    if (defined $self->{rpc} and defined $self->{order_return}->{$uuid}) {
        if (defined $private) {
            $self->{rpc}->call($self->{order_return}->{$uuid}, 'result', $uuid, $module, $payload, $private);
        }
        else {
            $self->{rpc}->call($self->{order_return}->{$uuid}, 'result', $uuid, $module, $payload);
        }
    }

    return;
}

=head2 function2

=cut

sub _del_worker {
    my ($self, $worker) = @_;

    ## check if current work, put back in queue

    delete $self->{worker}->{$worker->uri};
}

=head2 function2

=cut

sub _workers {
    my ($self) = @_;
    my (@o, $timeout);
    my $dispatcher = $self->{dispatcher}->{$self->{rpc}->uri};

    foreach my $worker (values %{$self->{worker}}) {
        unless (exists $dispatcher->worker->{$worker->uri}) {
            $self->_del_worker($worker);
            $self->{logger}->info('lost worker ', $worker->uri);
        }
    }

    foreach my $worker_uri (keys %{$dispatcher->worker}) {
        unless (exists $self->{worker}->{$worker_uri}) {
            my ($wself, $signal, $error, $rpc);

            $wself = $self; weaken($wself);
            $timeout = 0;
            $signal = Coro::Signal->new;
            push @o, AnyEvent->timer(
                after => 30,
                cb => sub { $timeout = 1; $signal->send; });
            
            $rpc = PollMonster::Worker::Client->new(
                uri => $worker_uri,
                on_error => sub {
                    $wself->{logger}->warn('error from worker ', $_[0]->uri, ': ', $_[1]);
                    delete $wself->{worker}->{$_[0]->uri};

                    if (defined $signal) {
                        $error = $_[1];
                        $signal->send;
                    }
                },
                on_eof => sub {
                    delete $wself->{worker}->{$_[0]->uri};
                },
                on_connect => sub {
                    $_[0]->ready;
                    if (defined $signal) {
                        $signal->send;
                    }
                },
                service => {
                    ready => sub {
                        if (defined (my $payload = shift(@{$wself->{queue}}))) {
                            unless ($_[0]->work(@$payload)) {
                                $wself->{logger}->warn('error sending work to worker ', $_[0]->uri);
                                unshift(@{$wself->{queue}}, $payload);
                                delete $wself->{worker}->{$_[0]->uri};
                                $wself->{queue_signal}->broadcast;
                                return;
                            }
                            else {
                                $wself->_queue_notify;
                            }
                            return;
                        }

                        #weaken($rpc);
                        push(@{$wself->{ready_list}}, $_[0]->uri);
                        return;
                    },
                    result_ready => sub {
                        my $rpc = shift;

                        $wself->result(@_);

                        if (defined (my $payload = shift(@{$wself->{queue}}))) {
                            unless ($rpc->work(@$payload)) {
                                $wself->{logger}->warn('error sending work to worker ', $rpc->uri);
                                unshift(@{$wself->{queue}}, $payload);
                                delete $wself->{worker}->{$rpc->uri};
                                $wself->{queue_signal}->broadcast;
                                return;
                            }
                            else {
                                $wself->_queue_notify;
                            }
                            return;
                        }

                        #weaken($rpc);
                        push(@{$wself->{ready_list}}, $rpc->uri);
                        return;
                    }
                });
            $signal->wait;
            @o = ();
            $signal = undef;

            if ($timeout) {
                $self->{logger}->warn('timeout connecting to worker ', $rpc->uri);
                next;
            }

            if (defined $error) {
                $self->{logger}->warn('error connecting to worker ', $rpc->uri, ': ', $error);
                next;
            }

            my $worker = PollMonster::WorkDispatcher::Worker->new(
                uri => $rpc->uri,
                rpc => $rpc
                );
            $self->{worker}->{$worker->uri} = $worker;

            $self->{logger}->info('new worker ', $worker->uri);
        }
    }

    $self->{queue_signal}->broadcast;

    return;
}

=head2 function2

=cut

sub call_sync_dispatchers {
    my ($self, $rpc, $cli, @dispatchers) = @_;

    $self->{dispatcher} = {};
    foreach my $dispatcher (@dispatchers) {
        unless (ref($dispatcher) eq 'HASH') {
            $self->{logger}->warn('unable to sync dispatchers, data corrupt');
            return;
        }
        $self->{dispatcher}->{$dispatcher->{uri}} = PollMonster::WorkDispatcher::Dispatcher->new(
            %$dispatcher
            );
    }

    $self->_workers;

    return;
}

=head2 function2

=cut

sub _loadbalance_dispatchers {
    my ($self) = @_;
    my (@o, @list, $timeout, $signal);

    unless ($self->is_registered and defined PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME)) {
        return;
    }

    unless (defined $self->{rpc} and $self->{rpc}->is_oldest) {
        return;
    }

    $signal = Coro::Signal->new;
    push @o, AnyEvent->timer(
        after => 30,
        cb => sub { $timeout = 1; $signal->send; });
    push @o, PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub {
            $signal->send;
        },
        on_connect => sub {
            $_[0]->list(WORKER_NAME);
        },
        service => {
            list => sub {
                (undef, @list) = @_;
                $signal->send;
            }
        });
    $signal->wait;
    @o = ();

    if ($timeout) {
        $self->{logger}->warn('timeout retriving workers');
        return;
    }

    my $dispatcher_count = 0;
    my %worker_dispatcher = map { my $d = $_; $dispatcher_count++; map { $_ => $d->uri } %{$_->{worker}}; } values %{$self->{dispatcher}};
    my $max = int(scalar @list / ($dispatcher_count ? $dispatcher_count : 1)) + 1;

    $self->{logger}->info(scalar @list, ' workers, ', $dispatcher_count, ' dispatchers, max/d ', $max);

    foreach (values %{$self->{dispatcher}}) {
        $_->set_worker({});
    }

    my $changed = 0;
    foreach my $entry (@list) {
        if (exists $worker_dispatcher{$entry->{uri}}) {
            my $dispatcher = $self->{dispatcher}->{$worker_dispatcher{$entry->{uri}}};

            if ($dispatcher->num_workers < $max) {
                $dispatcher->add_worker($entry->{uri}, 1);
                next;
            }
        }

        my ($dispatcher) = sort { $a->num_workers <=> $b->num_workers } values %{$self->{dispatcher}};
        $dispatcher->add_worker($entry->{uri}, 1);
        $changed = 1;
    }

    if ($changed) {
        $self->{rpc}->push_sync('sync-dispatchers', values %{$self->{dispatcher}});
    }

    $self->_workers;

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
