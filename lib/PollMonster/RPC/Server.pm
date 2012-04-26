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

package PollMonster::RPC::Server;

use common::sense;
use Carp;

use PollMonster;
use PollMonster::RPC::Auth;
use PollMonster::RPC::Fault;
use PollMonster::RPC::Server::Client;

use Log::Log4perl ();

use Coro ();
use Coro::Channel ();
use Coro::Semaphore ();
use AnyEvent::Handle ();
use AnyEvent::Socket ();

use JSON::XS ();
use Digest::SHA qw(hmac_sha256_base64);
use Scalar::Util qw(blessed weaken);
use POSIX qw(getpid);
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
        client => {},
        service => {},
        channel => Coro::Channel->new,
        json => JSON::XS->new->ascii->convert_blessed(1)
    };
    bless $self, $class;
    my $rself = $self;
    weaken($self);

    unless (defined $args{name}) {
        croak __PACKAGE__.' arguments name missing';
    }
    $self->{name} = $args{name};

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

                unless (-d $port) {
                    croak 'bad uri: path is not a directory';
                }

                $port =~ s/\/*$//o;
                $port = $port.'/'.$self->{name}.'.'.getpid;

                if (-e $port) {
                    croak 'bad uri: file '.$port.' already exists';
                }
            }

            $self->{socket} = AnyEvent::Socket::tcp_server $host, $port, sub {
                my ($fh, $host, $port) = @_;
                my ($handle, $json, $challenge);

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

                $json = JSON::XS->new->ascii->convert_blessed(1);
                $handle = AnyEvent::Handle->new(
                    fh => $fh,
                    no_delay => exists $args{no_delay} ? $args{no_delay} : PollMonster::NO_DELAY,
                    autocork => exists $args{autocork} ? $args{autocork} : PollMonster::AUTOCORK,
                    read_size => PollMonster::READ_SIZE,
                    rbuf_max => PollMonster::READ_BUF,
                    keepalive => 1,
                    linger => 0,
                    on_error => sub {
                        my ($handle, $fatal, $message) = @_;

                        $self->{logger}->warn('rpc client error ', $message);

                        if (exists $self->{on_error}) {
                            $self->{on_error}->($self, $self->{client}->{$handle}, $message);
                        }

                        delete $self->{client}->{$handle};
                        $handle->destroy;
                    },
                    on_eof => sub {
                        my ($handle) = @_;

                        PollMonster::DEBUG and exists $self->{client}->{$handle} and
                            $self->{logger}->debug('rpc client ', $self->{client}->{$handle}->uri, ' disconnect from ', $self->uri);
                        PollMonster::INFO and !exists $self->{client}->{$handle} and
                            $self->{logger}->info('rpc client unknown (', $handle, ') disconnect from ', $self->uri);

                        if (exists $self->{on_eof}) {
                            $self->{on_eof}->($self, $self->{client}->{$handle});
                        }
                        elsif (exists $self->{on_error}) {
                            $self->{on_error}->($self, $self->{client}->{$handle}, 'EOF');
                        }

                        delete $self->{client}->{$handle};
                        $handle->destroy;
                    },
                    on_read => sub {
                        my ($handle) = @_;
                        
                        $json->incr_parse($handle->{rbuf});
                        while () {
                            my $ref;

                            eval { $ref = $json->incr_parse };
                            
                            if (ref($ref) eq 'ARRAY') {
                                weaken($handle);
                                unshift(@$ref, $handle);
                                $self->{channel}->put($ref);
                            }
                            else {
                                last;
                            }
                        }

                        if ($@) {
                            # error case
                            $json->incr_skip;
                            
                            $handle->{rbuf} = $json->incr_text;
                            $json->incr_text = '';

                            $handle->_error (Errno::EBADMSG);
                        }
                        else {
                            $handle->{rbuf} = '';
                        }
                    }
                    );

                $challenge = '<'; 
                $challenge .= int(rand(10)) for (1..10);
                $challenge .= '.'.time().'@'.$self->{uri}.'>';

                $self->{client}->{$handle} = PollMonster::RPC::Server::Client->new(
                    uri => $proto.'://'.$host.':'.$port,
                    handle => $handle,
                    challenge => $challenge
                    );

                if (exists $args{autocork} ? $args{autocork} : PollMonster::AUTOCORK) {
                    my $wrote = Coro::Semaphore->new(PollMonster::CHUNK_BUF);
                    $self->{client}->{$handle}->set_wrote($wrote);

                    weaken($wrote);
                    $handle->on_drain(
                        sub {
                            $wrote->adjust(PollMonster::CHUNK_BUF - $wrote->count);
                        });
                }

                PollMonster::DEBUG and $self->{logger}->debug('rpc client ', $self->{client}->{$handle}->uri, ' connect to ', $self->{uri});

                PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ _version, ', $VERSION, ', ', $challenge, ' ]');
                $handle->push_write($self->{json}->encode([ '_version', $VERSION, $challenge ]));
            }, sub {
                my (undef, $host, $port) = @_;

                if ($proto eq 'unix') {
                    $self->{uri} = $proto.'://'.$port;
                }
                else {
                    $self->{uri} = $proto.'://'.$host.':'.$port;
                }
                
                PollMonster::DEBUG and $self->{logger}->debug('rpc server ready at ', $self->{uri});

                if (exists $args{prepare} and ref($args{prepare}) eq 'CODE') {
                    $args{prepare}->($self, @_);
                }

                PollMonster::SRV_LISTEN;
            };
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

    if (defined $self->{channel}) {
        $self->{channel}->shutdown;
    }

    if (defined $self->{socket} and $self->{uri} =~ /^unix:\/\/(.+)/o) {
        my $file = $1;

        if (-S $file) {
            unlink($file);
        }
    }
}

=head2 function2

=cut

sub called {
    my ($self, $ref) = @_;
    my ($handle, $method, @parameters) = @$ref;

    unless (exists $self->{client}->{$handle}) {
        $self->{logger}->warn('called from unknown client (', $handle, ')');
        return;
    }

    PollMonster::RPC_DEBUG and $self->{logger}->debug('<= [ ', $method, ', ', @parameters ? join(', ', @parameters) : '()', ' ]');

    my $cli = $self->{client}->{$handle};

    if (substr($method, 0, 1) eq '_') {
        if ($method eq '_auth') {
            unless ($parameters[0] eq hmac_sha256_base64($cli->challenge, PollMonster::RPC::Auth->instance->api_key)) {
                $self->{logger}->warn('client ', $cli->uri, ' failed auth');

                PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ _noauth ]');
                $handle->push_write($self->{json}->encode([ '_noauth' ]));

                if (exists $self->{on_noauth}) {
                    $self->{on_noauth}->($self, $cli);
                }
                elsif (exists $self->{on_error}) {
                    $self->{on_error}->($self, $cli, 'NOAUTH');
                }

                delete $self->{client}->{$handle};
                $handle->push_shutdown;
                return;
            }

            $cli->set_authed(1);

            PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ _authed, ', $self->{name}, ' ]');
            $handle->push_write($self->{json}->encode([ '_authed', $self->{name} ]));

            if (exists $self->{on_connect}) {
                $self->{on_connect}->($self, $cli);
            }
            return;
        }
        elsif ($method eq '_err') {
            return;
        }
        else {
            $self->{logger}->warn('unknown rpc call ', $method);

            if (exists $self->{on_error}) {
                $self->{on_error}->($self, $cli, 'unknown rpc call');
            }

            delete $self->{client}->{$handle};
            $handle->push_shutdown;
            return;
        }
    }

    unless ($cli->is_connected) {
        PollMonster::INFO and $self->{logger}->info('got call but client isnt connected fully');
        return;
    }

    unless (exists $self->{service}->{$method}) {
        PollMonster::RPC_DEBUG and $self->{logger}->debug('-> [ _err, ', $method, ', ', PollMonster::ENOMETHOD, ' ]');
        $handle->push_write($self->{json}->encode([ '_err', $method, PollMonster::ENOMETHOD ]));
        return;
    }

    $self->{service}->{$method}->($self, $cli, @parameters);

    return;
}

=head2 function2

=cut

sub call {
    my ($self, $cli, $method, @parameters) = @_;

    unless (blessed($cli) and $cli->isa('PollMonster::RPC::Server::Client') and exists $self->{client}->{$cli->handle}) {
        return;
    }

    unless ($cli->is_connected) {
        return;
    }

    if ($cli->wrote) {
        $cli->wrote->down;
    }

    if (blessed($method) and $method->isa('PollMonster::RPC::Fault')) {
        PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ _err, ', $method->method, ', ', $method->errno, ', ', $method->errstr, ' ]');
        $cli->handle->push_write($self->{json}->encode($method));
    }
    else {
        PollMonster::RPC_DEBUG and $self->{logger}->debug('=> [ ', $method, ', ', @parameters ? join(', ', @parameters) : '()', ' ]');
        $cli->handle->push_write($self->{json}->encode([ $method, @parameters ]));
    }

    return;
}

=head2 function2

=cut

sub close {
    my ($self) = @_;

    $self->{socket} = undef;
    $self->{client} = {};

    $self;
}

=head2 function2

=cut

sub uri {
    $_[0]->{uri};
}

=head2 function2

=cut

sub name {
    $_[0]->{name};
}

=head2 function2

=cut

sub is_alive {
    defined $_[0]->{socket};
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
