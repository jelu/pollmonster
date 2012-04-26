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

package PollMonster::RPC::Network;

use common::sense;
use Carp;

use base qw(PollMonster::RPC::Server);

use PollMonster;
use PollMonster::RPC::Fault;
use PollMonster::RPC::Client;
use PollMonster::RPC::Network::Client;

use Log::Log4perl ();
use Time::HiRes ();
use Scalar::Util qw(blessed weaken);

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;

=head1 SYNOPSIS

This is the RPC module for PollMonster.

=head1 SUBROUTINES/METHODS

=head2 function1

=cut

sub new {
    my $this = shift;
    my $class = ref($this) || $this;
    my %args = ( @_ );
    my $self = $class->SUPER::new(@_);
    $self->{net_logger} = Log::Log4perl->get_logger;
    $self->{net_joined} = 0;
    $self->{net_client} = {};
    $self->{net_client_uri} = {};
    $self->{net_ctime} = Time::HiRes::time;
    bless $self, $class;
    my $rself = $self;
    weaken($self);

    $self->{service}->{'net-join'} = sub { $self->call_net_join(@_); };
    $self->{service}->{'net-sync-clients'} = sub { $self->call_net_sync_clients(@_); };
    $self->{service}->{'net-sync-join'} = sub { $self->call_net_sync_join(@_); };
    $self->{service}->{'net-sync-leave'} = sub { $self->call_net_sync_leave(@_); };

    if (exists $args{on_net_join} and ref($args{on_net_join}) eq 'CODE') {
        $self->{net_on_join} = $args{on_net_join};
    }
    if (exists $args{on_net_leave} and ref($args{on_net_leave}) eq 'CODE') {
        $self->{net_on_leave} = $args{on_net_leave};
    }

    PollMonster::OBJ_DEBUG and $self->{net_logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and $_[0]->{net_logger}->debug('destroy ', __PACKAGE__, ' ', $_[0]);

    $_[0]->SUPER::DESTROY;
}

=head2 function2

=cut

sub net_join {
    my ($self, $uri, $callback) = @_;

    unless (ref($callback) eq 'CODE') {
        $callback = undef;
    }

    PollMonster::RPC_DEBUG and $self->{net_logger}->debug('joining network at ', $uri);

    if ($uri eq $self->uri or exists $self->{net_client_uri}->{$uri}) {
        $self->{net_joined} = 1;

        if (defined $callback) {
            $callback->($self, $uri);
        }
        return;
    }   

    my $net_cli = PollMonster::RPC::Network::Client->new(
        uri => $uri
        );

    $self->{net_client_uri}->{$uri} = $net_cli;

    weaken($self);
    weaken($net_cli);

    my $net_rpc = PollMonster::RPC::Client->new(
        uri => $uri,
        on_error => sub {
            $self->net_on_error(@_);

            if (defined $callback) {
                $callback->($self, $uri);
                $callback = undef;
            }
        },
        on_eof => sub {
            $self->net_on_eof(@_);

            if (defined $callback) {
                $callback->($self, $uri);
                $callback = undef;
            }
        },
        on_noauth => sub {
            $self->net_on_noauth(@_);

            if (defined $callback) {
                $callback->($self, $uri);
                $callback = undef;
            }
        },
        on_connect => sub {
            my ($rpc, $name) = @_;

            unless ($self->is_alive) {
                $self->{net_logger}->warn('client ', $rpc->uri, ' can not join: server closed');
                $self->net_close_rpc($rpc);
                return;
            }

            unless ($name eq $self->{name}) {
                $self->{net_logger}->warn('client ', $rpc->uri, ' can not join: missmatch service name');
                $self->net_close_rpc($rpc);
                return;
            }

            $rpc->call('net-join', $self->uri, $self->{net_ctime});
        },
        service => {
            'net-join' => sub {
                my ($rpc, $ctime) = @_;

                if (blessed($ctime) and $ctime->isa('PollMonster::RPC::Fault')) {
                    if ($ctime->errno == PollMonster::ENETCTIME and $self->is_alive) {
                        $self->{net_ctime} = Time::HiRes::time;
                        $rpc->call('net-join', $self->uri, $self->{net_ctime});
                        return;
                    }

                    $self->{net_logger}->warn('client ', $rpc->uri, ' unable to join: ', $ctime->errstr);
                    $self->net_close_rpc($rpc);
                    return;
                }

                unless (defined $ctime) {
                    $self->{net_logger}->warn('client ', $rpc->uri, ' did not return ctime');
                    $self->net_close_rpc($rpc);
                    return;
                }

                PollMonster::RPC_DEBUG and $self->{net_logger}->debug('joined network at ', $uri);

                $net_cli->set_ctime($ctime);
                $net_cli->set_connected(1);
                $self->_flush_sync_rpc($rpc);
                $self->{net_joined} = 1;

                if (defined $callback) {
                    $callback->($self, $uri);
                    $callback = undef;
                }

                if (exists $self->{net_on_join}) {
                    $self->{net_on_join}->($self, $net_cli, $uri);
                }
            }
        }
        );
    $self->{net_client}->{$net_rpc} = $net_cli;

    $net_cli->set_rpc($net_rpc);

    my %net_client = map { $_->uri => $_->ctime } values %{$self->{net_client}};
    $self->_push_sync_rpc($net_rpc, 'net-sync-clients', \%net_client);

    return;
}

=head2 function2

=cut

sub net_close_rpc {
    my ($self, $rpc) = @_;

    if (exists $self->{net_client}->{$rpc}) {
        my $cli = delete $self->{net_client}->{$rpc};
        delete $self->{net_client_uri}->{$cli->uri};

        $self->push_sync('net-sync-leave', $cli->uri);

        if (exists $self->{net_on_leave}) {
            $self->{net_on_leave}->($self, $cli->uri);
        }

        $cli->rpc->close;
    }

    return;
}

=head2 function2

=cut

sub net_on_error {
    my ($self, $rpc, $handle) = @_;

    $self->net_close_rpc($rpc);
}

=head2 function2

=cut

sub net_on_eof {
    my ($self, $rpc, $handle) = @_;

    $self->net_close_rpc($rpc);
}

=head2 function2

=cut

sub net_on_noauth {
    my ($self, $rpc, $handle) = @_;

    $self->net_close_rpc($rpc);
}

=head2 function2

=cut

sub close {
    my ($self) = @_;

    $self->push_sync('net-sync-leave', $self->uri);

    $self->{net_client} = {};
    $self->{net_client_uri} = {};
    $self->close;

    $self;
}

=head2 function2

=cut

sub call_net_join {
    my ($self, $rpc, $cli, $uri, $ctime) = @_;

    if (exists $self->{net_client_uri}->{$uri}) {
        my %net_client = map { $_->uri => $_->ctime } values %{$self->{net_client}};
        $self->{net_client_uri}->{$uri}->rpc->call('net-sync-clients', \%net_client);

        PollMonster::RPC_DEBUG and $self->{net_logger}->debug('client ', $uri, ' joining network (but already connected)');

        $rpc->call($cli, 'net-join', $self->{net_ctime});
        return;
    }

    if ($self->{net_ctime} == $ctime) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(method => 'net-join', errno => PollMonster::ENETCTIME));
        return;
    }

    foreach (values %{$self->{net_client}}) {
        if ($_->ctime == $ctime) {
            $rpc->call($cli, PollMonster::RPC::Fault->new(method => 'net-join', errno => PollMonster::ENETCTIME));
            return;
        }
    }

    PollMonster::RPC_DEBUG and $self->{net_logger}->debug('client ', $uri, ' joining network');

    my $net_cli = PollMonster::RPC::Network::Client->new(
        uri => $uri,
        ctime => $ctime
        );

    $self->{net_client_uri}->{$uri} = $net_cli;

    weaken($self);
    weaken($net_cli);

    my $net_rpc = PollMonster::RPC::Client->new(
        uri => $uri,
        on_error => sub { $self->net_on_error(@_); },
        on_eof => sub { $self->net_on_eof(@_); },
        on_noauth => sub { $self->net_on_noauth(@_); },
        on_connect => sub {
            my ($rpc, $name) = @_;

            unless ($self->is_alive) {
                $self->{net_logger}->warn('client ', $rpc->uri, ' can not be added: server closed');
                $self->net_close_rpc($rpc);
                return;
            }

            unless ($name eq $self->{name}) {
                $self->{net_logger}->warn('client ', $rpc->uri, ' can not be added: missmatch service name');
                $self->net_close_rpc($rpc);
                return;
            }

            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('adding client ', $uri, ' to network');

            $net_cli->set_connected(1);
            $self->_flush_sync_rpc($rpc);
            $self->push_sync('net-sync-join', $uri, $ctime);

            if (exists $self->{net_on_join}) {
                $self->{net_on_join}->($self, $net_cli, $uri);
            }
        }
        );
    $self->{net_client}->{$net_rpc} = $net_cli;

    $net_cli->set_rpc($net_rpc);

    my %net_client = map { $_->uri => $_->ctime } values %{$self->{net_client}};
    $self->_push_sync_rpc($net_rpc, 'net-sync-clients', \%net_client);

    $rpc->call($cli, 'net-join', $self->{net_ctime});

    return;
}

=head2 function2

=cut

sub _new_net_cli {
    my ($self, $uri, $ctime) = @_;

    my $net_cli = PollMonster::RPC::Network::Client->new(
        uri => $uri,
        ctime => $ctime
        );

    $self->{net_client_uri}->{$uri} = $net_cli;

    weaken($self);
    weaken($net_cli);

    my $net_rpc = PollMonster::RPC::Client->new(
        uri => $uri,
        on_error => sub { $self->net_on_error(@_); },
        on_eof => sub { $self->net_on_eof(@_); },
        on_noauth => sub { $self->net_on_noauth(@_); },
        on_connect => sub {
            my ($rpc, $name) = @_;

            unless ($self->is_alive) {
                $self->{net_logger}->warn('client ', $rpc->uri, ' can not be added: server closed');
                $self->net_close_rpc($rpc);
                return;
            }

            unless ($name eq $self->{name}) {
                $self->{net_logger}->warn('client ', $rpc->uri, ' can not be added: missmatch service name');
                $self->net_close_rpc($rpc);
                return;
            }

            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('adding client ', $uri, ' to network');
            
            $net_cli->set_connected(1);
            $self->_flush_sync_rpc($rpc);

            if (exists $self->{net_on_join}) {
                $self->{net_on_join}->($self, $net_cli, $uri);
            }
        }
        );
    $self->{net_client}->{$net_rpc} = $net_cli;

    $net_cli->set_rpc($net_rpc);

    my %net_client = map { $_->uri => $_->ctime } values %{$self->{net_client}};
    $self->_push_sync_rpc($net_rpc, 'net-sync-clients', \%net_client);

    $self;
}

=head2 function2

=cut

sub call_net_sync_clients {
    my ($self, $rpc, $cli, $net_clients) = @_;

    unless (ref($net_clients) eq 'HASH') {
        return;
    }

    while (my ($uri, $ctime) = each %$net_clients) {
        if ($uri eq $self->uri or exists $self->{net_client_uri}->{$uri}) {
            next;
        }   

        $self->_new_net_cli($uri, $ctime);
    }

    return;
}

=head2 function2

=cut

sub call_net_sync_join {
    my ($self, $rpc, $cli, $uri, $ctime) = @_;

    if ($uri eq $self->uri or exists $self->{net_client_uri}->{$uri}) {
        return;
    }   

    $self->_new_net_cli($uri, $ctime);

    return;
}

=head2 function2

=cut

sub call_net_sync_leave {
    my ($self, $rpc, undef, $uri) = @_;

    PollMonster::RPC_DEBUG and $self->{net_logger}->debug('client ', $uri, ' leaving network');

    if (exists $self->{net_client_uri}->{$uri}) {
        my $cli = delete $self->{net_client_uri}->{$uri};
        delete $self->{net_client}->{$cli->rpc};

        if (exists $self->{net_on_leave}) {
            $self->{net_on_leave}->($self, $cli->uri);
        }
    }

    return;
}

=head2 function2

=cut

sub push_sync {
    my ($self, @sync) = @_;

    foreach my $cli (values %{$self->{net_client}}) {
        if ($cli->connected) {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('sync ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->rpc->call(@sync)
        }
        else {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('buffer ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->add_sync(\@sync);
        }
    }
    
    $self;
}

=head2 function2

=cut

sub push_sync_cli {
    my ($self, $cli, @sync) = @_;

    if (exists $self->{net_client}->{$cli->rpc}) {
        if ($cli->connected) {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('sync ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->rpc->call(@sync);
        }
        else {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('buffer ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->add_sync(\@sync);
        }
    }

    $self;
}

=head2 function2

=cut

sub push_sync_uri {
    my ($self, $uri, @sync) = @_;

    if (exists $self->{net_client_uri}->{$uri}) {
        my $cli = $self->{net_client_uri}->{$uri};

        if ($cli->connected) {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('sync ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->rpc->call(@sync);
        }
        else {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('buffer ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->add_sync(\@sync);
        }
    }

    $self;
}

=head2 function2

=cut

sub _push_sync_rpc {
    my ($self, $net_rpc, @sync) = @_;

    if (exists $self->{net_client}->{$net_rpc}) {
        my $cli = $self->{net_client}->{$net_rpc};

        if ($cli->connected) {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('sync ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->rpc->call(@sync);
        }
        else {
            PollMonster::RPC_DEBUG and $self->{net_logger}->debug('buffer ', $cli->uri, ' [ ', join(', ', @sync), ' ]');
            $cli->add_sync(\@sync);
        }
    }

    $self;
}

=head2 function2

=cut

sub _flush_sync_rpc {
    my ($self, $net_rpc) = @_;

    if (exists $self->{net_client}->{$net_rpc}) {
        my $cli = $self->{net_client}->{$net_rpc};

        if ($cli->connected) {
            foreach my $sync (@{$cli->sync}) {
                PollMonster::RPC_DEBUG and $self->{net_logger}->debug('flush ', $cli->uri, ' [ ', join(', ', @$sync), ' ]');
                $cli->rpc->call(@$sync);
            }
            $cli->set_sync([]);
        }
    }

    $self;
}

=head2 function2

=cut

sub is_joined {
    $_[0]->{net_joined};
}

=head2 function2

=cut

sub is_oldest {
    foreach (values %{$_[0]->{net_client}}) {
        if ($_->ctime < $_[0]->{net_ctime}) {
            return;
        }
    }

    return 1;
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
