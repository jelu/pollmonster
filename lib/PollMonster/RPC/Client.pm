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

package PollMonster::RPC::Client;

use common::sense;
use Carp;

use PollMonster;
use PollMonster::RPC::Auth;
use PollMonster::RPC::Fault;

use Log::Log4perl ();

use Coro ();
use Coro::Channel ();
use Coro::Signal ();
use Coro::Semaphore ();
use AnyEvent::Handle ();
use AnyEvent::Socket ();

use JSON::XS ();
use Digest::SHA qw(hmac_sha256_base64);
use Scalar::Util qw(weaken);
use Socket qw(:all);

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
    my $self = {
        logger => Log::Log4perl->get_logger,
        uri => '(none)',
        service => {},
        channel => Coro::Channel->new,
        authed => 0,
        json => JSON::XS->new->ascii->convert_blessed(1)
    };
    bless $self, $class;
    my $rself = $self;
    weaken($self);

    if (exists $args{autocork} ? $args{autocork} : PollMonster::AUTOCORK) {
        $self->{wrote} = Coro::Semaphore->new(PollMonster::CHUNK_BUF);
    }

    if (exists $args{on_connect} and ref($args{on_connect}) eq 'CODE') {
        $self->{on_connect} = $args{on_connect};
    }
    if (exists $args{on_error} and ref($args{on_error}) eq 'CODE') {
        $self->{on_error} = $args{on_error};
    }
    if (exists $args{on_eof} and ref($args{on_eof}) eq 'CODE') {
        $self->{on_eof} = $args{on_eof};
    }
    if (exists $args{on_noauth} and ref($args{on_noauth}) eq 'CODE') {
        $self->{on_noauth} = $args{on_noauth};
    }

    if (exists $args{on_service} and ref($args{on_service}) eq 'CODE') {
        $self->{on_service} = $args{on_service};
    }
    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach my $method (keys %{$args{service}}) {
            unless (ref($args{service}->{$method}) eq 'CODE') {
                croak 'method ', $method, ' is not a code ref';
            }
        }

        $self->{service} = $args{service};
    }

    if (exists $args{uri}) {
        my $coro = Coro::async {
            my $channel = $self->{channel};

            $Coro::current->prio(Coro::PRIO_HIGH);

            while (defined (my $ref = $channel->get) and defined $self) {
                $self->called($ref);
            }
        };
        $coro->desc('Channel '.$rself);

        if ($args{uri} =~ /^(tcp):\/\/([a-zA-Z0-9\.-]+)(?::([0-9]+))*/o or
            $args{uri} =~ /^(unix):\/\/(.+)/o)
        {
            my ($proto, $host, $port) = ($1, $2, $3);

            if ($proto eq 'unix') {
                $port = $host;
                $host = 'unix/';
                $self->{uri} = $proto.'://'.$port;
            }
            else {
                $self->{uri} = $proto.'://'.$host.':'.$port;
            }

            if (exists $args{connect_wait}) {
                $self->{connect_signal} = Coro::Signal->new;
            }

            $self->{socket} = AnyEvent::Socket::tcp_connect $host, $port, sub {
                my ($fh) = @_;

                unless (defined $fh) {
                    $self->{logger}->warn('unable to connect to ', $self->{uri}, ': ', $!);

                    if (exists $self->{on_error}) {
                        $self->{on_error}->($self, $!);
                    }

                    if (exists $self->{connect_signal}) {
                        $self->{connect_signal}->send;
                    }
                    return;
                }

                my ($sndbuf, $rcvbuf);

                if (defined ($sndbuf = getsockopt($fh, SOL_SOCKET, SO_SNDBUF))) {
                    $sndbuf = unpack('I', $sndbuf);
                    if ($sndbuf < (exists $args{sndbuf} ? $args{sndbuf} : PollMonster::SNDBUF)) {
                        setsockopt($fh, SOL_SOCKET, SO_SNDBUF, exists $args{sndbuf} ? $args{sndbuf} : PollMonster::SNDBUF);
                    }
                }
                else {
                    setsockopt($fh, SOL_SOCKET, SO_SNDBUF, exists $args{sndbuf} ? $args{sndbuf} : PollMonster::SNDBUF);
                }

                if (defined ($rcvbuf = getsockopt($fh, SOL_SOCKET, SO_RCVBUF))) {
                    $rcvbuf = unpack('I', $rcvbuf);
                    if ($rcvbuf < (exists $args{rcvbuf} ? $args{rcvbuf} : PollMonster::RCVBUF)) {
                        setsockopt($fh, SOL_SOCKET, SO_RCVBUF, exists $args{rcvbuf} ? $args{rcvbuf} : PollMonster::RCVBUF);
                    }
                }
                else {
                    setsockopt($fh, SOL_SOCKET, SO_RCVBUF, exists $args{rcvbuf} ? $args{rcvbuf} : PollMonster::RCVBUF);
                }

                $self->{handle} = AnyEvent::Handle->new(
                    fh => $fh,
                    no_delay => exists $args{no_delay} ? $args{no_delay} : PollMonster::NO_DELAY,
                    autocork => exists $args{autocork} ? $args{autocork} : PollMonster::AUTOCORK,
                    read_size => PollMonster::READ_SIZE,
                    rbuf_max => PollMonster::READ_BUF,
                    keepalive => 1,
                    linger => 0,
                    on_error => sub {
                        my ($handle, $fatal, $message) = @_;

                        $self->{logger}->warn('rpc server error ', $message);

                        if (exists $self->{connect_signal}) {
                            $self->{connect_signal}->send;
                        }

                        if (exists $self->{on_error}) {
                            $self->{on_error}->($self, $message);
                        }

                        $handle->destroy;
                    },
                    on_eof => sub {
                        my ($handle) = @_;

                        PollMonster::DEBUG and $self->{logger}->debug('rpc server ', $self->{uri}, ' disconnect');

                        if (exists $self->{connect_signal}) {
                            $self->{connect_signal}->send;
                        }

                        if (exists $self->{on_eof}) {
                            $self->{on_eof}->($self);
                        }
                        elsif (exists $self->{on_error}) {
                            $self->{on_error}->($self, 'EOF');
                        }

                        $handle->destroy;
                    },
                    on_read => sub {
                        my ($handle) = @_;

                        $self->{json}->incr_parse($handle->{rbuf});
                        while () {
                            my $ref;

                            eval { $ref = $self->{json}->incr_parse };

                            if (ref($ref) eq 'ARRAY') {
                                $self->{channel}->put($ref);
                            }
                            else {
                                last;
                            }
                        }

                        if ($@) {
                            # error case
                            $self->{json}->incr_skip;
                            
                            $handle->{rbuf} = $self->{json}->incr_text;
                            $self->{json}->incr_text = "";

                            $handle->_error (Errno::EBADMSG);
                        }
                        else {
                            $handle->{rbuf} = "";
                        }
                    }
                    );

                if (exists $self->{wrote}) {
                    $self->{handle}->on_drain(
                        sub {
                            $self->{wrote}->adjust(PollMonster::CHUNK_BUF - $self->{wrote}->count);
                        });
                }

                PollMonster::DEBUG and $self->{logger}->debug('rpc server connecting');
            },
            sub {
                PollMonster::CLI_TIMEOUT;
            };

            if (exists $self->{connect_signal}) {
                $self->{connect_signal}->wait;
                delete $self->{connect_signal};
            }
        }
        else {
            croak 'bad uri';
        }
    }

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    my ($self) = @_;

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('destroy ', __PACKAGE__, ' ', $self);

    if (defined $self->{handle}) {
        $self->{handle}->push_shutdown;
    }

    if (exists $self->{channel}) {
        $self->{channel}->shutdown;
    }
}

=head2 function2

=cut

sub called {
    my ($self, $ref) = @_;
    my ($method, @parameters) = @$ref;

    PollMonster::RPC_DEBUG and $self->{logger}->debug('<= [ ', $method, ', ', @parameters ? join(', ', @parameters) : '()', ' ]');

    if (substr($method, 0, 1) eq '_') {
        if ($method eq '_version') {
            unless ($parameters[0] == $VERSION) {
                $self->{logger}->warn('client/server version missmatch');

                if (exists $self->{on_error}) {
                    $self->{on_error}->($self, 'version missmatch');
                }

                if (exists $self->{connect_signal}) {
                    $self->{connect_signal}->send;
                }

                $self->{handle}->destroy;
                return;
            }
            PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ _auth, ... ]');
            $self->{handle}->push_write($self->{json}->encode([ '_auth', hmac_sha256_base64($parameters[1], PollMonster::RPC::Auth->instance->api_key) ]));
            return;
        }
        elsif ($method eq '_authed') {
            $self->{authed} = 1;
            PollMonster::DEBUG and $self->{logger}->debug('rpc server ', $self->{uri}, ' connect');

            if (exists $self->{on_connect}) {
                $self->{on_connect}->($self, @parameters);
            }

            if (exists $self->{connect_signal}) {
                $self->{connect_signal}->send;
            }
            return;
        }
        elsif ($method eq '_noauth') {
            PollMonster::DEBUG and $self->{logger}->debug('got noauth from server ', $self->{uri});

            if (exists $self->{on_noauth}) {
                $self->{on_noauth}->($self);
            }
            elsif (exists $self->{on_error}) {
                $self->{on_error}->($self, 'NOAUTH');
            }

            if (exists $self->{connect_signal}) {
                $self->{connect_signal}->send;
            }
            return;
        }
        elsif ($method eq '_err') {
            my ($errmethod, $errno, $errstr) = @parameters;

            if (defined $errmethod and exists $PollMonster::ERRSTR{$errno}) {
                @parameters = ( PollMonster::RPC::Fault->new(
                                method => $errmethod,
                                errno => $errno
                            ) );
                $method = $errmethod;
            }
            elsif (defined $errmethod and defined $errno and defined $errstr) {
                @parameters = ( PollMonster::RPC::Fault->new(
                                method => $errmethod,
                                errno => $errno,
                                errstr => $errstr
                            ) );
                $method = $errmethod;
            }
            else {
                $self->{logger}->warn('unknown rpc error');
                return;
            }
        }
        else {
            $self->{logger}->warn('unknown rpc call ', $method);

            if (exists $self->{on_error}) {
                $self->{on_error}->($self, 'unknown rpc call');
            }

            if (exists $self->{connect_signal}) {
                $self->{connect_signal}->send;
            }

            $self->{handle}->destroy;
            return;
        }
    }

    unless ($self->{authed}) {
        PollMonster::INFO and $self->{logger}->info('got call but server isnt connected fully');
        return;
    }

    unless (exists $self->{service}->{$method}) {
        return;
    }

    if (exists $self->{on_service}) {
        $self->{service}->{$method}->($self->{on_service}->($self), @parameters);
    }
    else {
        $self->{service}->{$method}->($self, @parameters);
    }

    return;
}

=head2 function2

=cut

sub call {
    my ($self, $method, @parameters) = @_;

    unless (defined $method) {
        croak 'missing parameters';
    }

    unless ($self->is_connected) {
        return;
    }

    if (exists $self->{wrote}) {
        $self->{wrote}->down;
    }

    PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ ', $method, ' [ ', join(', ', @parameters), ' ] ]');
    $self->{handle}->push_write($self->{json}->encode([ $method, @parameters ]));

    return 1;
}

=head2 function2

=cut

sub close {
    my ($self) = @_;

    $self->{socket} = undef;

    if (defined $self->{handle} and !$self->{handle}->destroyed) {
        $self->{handle}->push_shutdown;
    }

    $self;
}

=head2 function2

=cut

sub is_connected {
    defined $_[0]->{handle} and !$_[0]->{handle}->destroyed and $_[0]->{authed};
}

=head2 function2

=cut

sub uri {
    $_[0]->{uri};
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
