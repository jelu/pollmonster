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

package PollMonster::ProcessManager::Server;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Network;
use PollMonster::ProcessManager::Client;

use AnyEvent ();

use Log::Log4perl ();
use POSIX qw(getpid);
use Scalar::Util qw(weaken);
use Sys::Hostname ();

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
        logger => Log::Log4perl->get_logger,
        registered => 0,
        process => {},
        heartbeat => {}
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

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(register unregister list sync-processes sync-add-process sync-remove-process)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{register} = sub { $self->call_register(@_) };
    $args{service}->{unregister} = sub { $self->call_unregister(@_) };
    $args{service}->{list} = sub { $self->call_list(@_) };
    $args{service}->{heartbeat} = sub { $self->call_heartbeat(@_) };
    $args{service}->{'sync-processes'} = sub { $self->call_sync_processes(@_) };
    $args{service}->{'sync-add-process'} = sub { $self->call_sync_add_process(@_) };
    $args{service}->{'sync-remove-process'} = sub { $self->call_sync_remove_process(@_) };
    $args{service}->{'sync-heartbeats'} = sub { $self->call_sync_heartbeats(@_) };

    $args{name} = PROCESS_MANAGER_NAME;
    $self->{rpc} = PollMonster::RPC::Network->new(%args);

    $self->{process_key} =
        join(' ', PollMonster->CFG(GLOBAL_NAME, 'hostname', Sys::Hostname::hostname), PROCESS_MANAGER_NAME, $self->{rpc}->uri, getpid);

    $self->{process} = {
        $self->{process_key} => {
            hostname => PollMonster->CFG(GLOBAL_NAME, 'hostname', Sys::Hostname::hostname),
            service => PROCESS_MANAGER_NAME,
            uri => $self->{rpc}->uri,
            pid => getpid,
            heartbeat => time
        }
    };

    $self->{heartbeat_check_timer} = AnyEvent->timer(
        after => PollMonster::HEARTBEAT_INTERVAL,
        interval => PollMonster::HEARTBEAT_INTERVAL,
        cb => sub {
            if (defined $self->{rpc}) {
                $self->{process}->{$self->{process_key}}->{heartbeat} =
                    $self->{heartbeat}->{$self->{process_key}} =
                    time;
                $self->{rpc}->push_sync('sync-heartbeats', $self->{heartbeat});
                $self->{heartbeat} = {};

                my @remove;
                my $timeout = time - PollMonster::HEARTBEAT_TIMEOUT;
                foreach my $pkey (keys %{$self->{process}}) {
                    if ($self->{process}->{$pkey}->{service} eq PROCESS_MANAGER_NAME) {
                        next;
                    }

                    if ($self->{process}->{$pkey}->{heartbeat} < $timeout) {
                        my $process = delete $self->{process}->{$pkey};
                        push(@remove, [$process->{hostname}, $process->{service}, $process->{uri}, $process->{pid}]);
                    }
                }

                foreach my $remove (@remove) {
                    $self->{rpc}->push_sync('sync-remove-process', @$remove);
                }
            }
        });

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    my ($self) = @_;

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('destroy ', __PACKAGE__, ' ', $self);

    if ($self->is_registered and defined PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME)) {
        $self->unregister(PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME));
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
            $self->{rpc}->net_join($uri);

            if (defined $callback) {
                $callback->($self);
            }
        },
        0
        );
}

=head2 function2

=cut

sub close {
    my ($self) = @_;
    my $id = ${$self};

    $self->{rpc} = undef;
    $self->{process} = {};

    $self;
}

=head2 function2

=cut

sub call_register {
    my ($self, $rpc, $cli, $hostname, $service, $uri, $pid) = @_;
    my $pkey = join(' ', $hostname, $service, $uri, $pid);

    if (defined $self->{process}->{$pkey}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'register',
                       errno => 1,
                       errstr => $service.' at '.$uri.' pid '.$pid.' already registered'
                   ));
        return;
    }

    $self->{process}->{$pkey} = {
        hostname => $hostname,
        service => $service,
        uri => $uri,
        pid => $pid,
        heartbeat => time
    };

    $rpc->call($cli, 'register', 1);
    $rpc->push_sync('sync-add-process', $hostname, $service, $uri, $pid);

    return;
}

=head2 function2

=cut

sub call_unregister {
    my ($self, $rpc, $cli, $hostname, $service, $uri, $pid) = @_;
    my $pkey = join(' ', $hostname, $service, $uri, $pid);

    unless (defined $self->{process}->{$pkey}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'unregister',
                       errno => 1,
                       errstr => $service.' at '.$uri.' pid '.$pid.' is not registered'
                   ));
        return;
    }

    delete $self->{process}->{$pkey};

    $rpc->call($cli, 'unregister', 1);
    $rpc->push_sync('sync-remove-process', $hostname, $service, $uri, $pid);

    return;
}

=head2 function2

=cut

sub call_list {
    my ($self, $rpc, $cli, $hostname, $service) = @_;
    my (@list);

    foreach (values %{$self->{process}}) {
        if (defined $service and $_->{service} ne $service) {
            next;
        }

        if ($_->{uri} =~ /^(?:unix|tcp:\/\/127.0.0.1)/o and $_->{hostname} ne $hostname) {
            # We do not include unix socket services unless they are local
            next;
        }

        push(@list, $_);
    }

    $rpc->call($cli, 'list', @list);
    return;
}

=head2 function2

=cut

sub call_heartbeat {
    my ($self, $rpc, $cli, $hostname, $service, $uri, $pid) = @_;
    my $pkey = join(' ', $hostname, $service, $uri, $pid);

    unless (defined $self->{process}->{$pkey}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'heartbeat',
                       errno => 1,
                       errstr => $service.' at '.$uri.' pid '.$pid.' is not registered'
                   ));
        return;
    }

    $self->{heartbeat}->{$pkey} = 
        $self->{process}->{$pkey}->{heartbeat} =
        time;

    $rpc->call($cli, 'heartbeat', 1);
    return;
}

=head2 function2

=cut

sub on_net_join {
    my ($self, $rpc, $cli, $uri) = @_;

    $rpc->push_sync_cli($cli, 'sync-processes', $self->{process});
}

=head2 function2

=cut

sub call_sync_processes {
    my ($self, $rpc, $cli, $processes) = @_;

    unless (ref($processes) eq 'HASH') {
        return;
    }

    while (my ($key, $value) = each %$processes) {
        unless (exists $self->{process}->{$key}) {
            $self->{process}->{$key} = $value;
        }
    }

    return;
}

=head2 function2

=cut

sub call_sync_add_process {
    my ($self, $rpc, $cli, $hostname, $service, $uri, $pid) = @_;
    my $pkey = join(' ', $hostname, $service, $uri, $pid);

    if (defined $self->{process}->{$pkey}) {
        return;
    }

    $self->{process}->{$pkey} = {
        hostname => $hostname,
        service => $service,
        uri => $uri,
        pid => $pid,
        heartbeat => time
    };

    return;
}

=head2 function2

=cut

sub call_sync_remove_process {
    my ($self, $rpc, $cli, $hostname, $service, $uri, $pid) = @_;

    delete $self->{process}->{join(' ', $hostname, $service, $uri, $pid)};

    return;
}

=head2 function2

=cut

sub call_sync_heartbeats {
    my ($self, $rpc, $cli, $heartbeat) = @_;

    unless (ref($heartbeat) eq 'HASH') {
        return;
    }

    foreach my $pkey (keys %$heartbeat) {
        if (exists $self->{process}->{$pkey} and $heartbeat->{$pkey} > $self->{process}->{$pkey}->{heartbeat}) {
            $self->{process}->{$pkey}->{heartbeat} = $heartbeat->{$pkey};
        }
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
