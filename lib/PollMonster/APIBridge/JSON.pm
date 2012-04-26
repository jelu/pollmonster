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

package PollMonster::APIBridge::JSON;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Server;

use AnyEvent::Handle ();
use AnyEvent::Socket ();

use HTTP::Status qw(:constants);
use HTTP::Request ();
use HTTP::Response ();
use URI ();
use URI::QueryParam ();

use JSON::XS ();
use Log::Log4perl ();
use Scalar::Util qw(weaken);

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;

sub MAX_REQUEST_LEN     (){ 64 * 1024 }

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
        client => {}
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
        foreach (qw(info)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{info} = sub { $self->call_info(@_) };

    $args{name} = APIBRIDGE_JSON_NAME;
    $self->{rpc} = PollMonster::RPC::Server->new(%args);

    if ($args{json_uri} =~ /^http:\/\/([a-zA-Z0-9\.-]+)(?::([0-9]+))*/o) {
        my ($host, $port) = ($1, $2);

        $self->{socket} = AnyEvent::Socket::tcp_server $host, $port, sub {
            my ($fh, $host, $port) = @_;
            my ($handle, $json);

            $json = JSON::XS->new->utf8;
            $handle = AnyEvent::Handle->new(
                fh => $fh,
                autocork => 0,
                linger => 0,
                on_error => sub {
                    my ($handle, $fatal, $message) = @_;

                    $self->{logger}->warn("$handle error: $message");

                    $handle->destroy;
                    delete $self->{client}->{$handle};
                },
                on_eof => sub {
                    my ($handle) = @_;

                    $self->{logger}->warn("$handle eof");

                    $handle->destroy;
                    delete $self->{client}->{$handle};
                },
                on_read => sub {
                    my ($handle) = @_;

                    unless (exists $self->{client}->{$handle}) {
                        $handle->push_shutdown;
                        $handle->destroy;
                        $handle->{rbuf} = '';
                        return;
                    }

                    if ((length($self->{client}->{$handle}->{rbuf}) + length($handle->{rbuf})) > MAX_REQUEST_LEN) {
                        $handle->push_shutdown;
                        $handle->destroy;
                        $handle->{rbuf} = '';
                        return;
                    }

                    $self->{client}->{$handle}->{rbuf} .= $handle->{rbuf};
                    $handle->{rbuf} = '';

                    if ($self->{client}->{$handle}->{rbuf} =~ /\r\n\r\n$/o) {
                        Coro::async_pool {
                            my $request = HTTP::Request->parse($self->{client}->{$handle}->{rbuf});
                            my $response = HTTP::Response->new;
                            $response->request($request);
                            $response->protocol($request->protocol);

                            my $query;
                            if ($request->header('Content-Type') eq 'application/x-www-form-urlencoded') {
                                my $query_str = $request->content;
                                $query_str =~ s/[\r\n]+$//o;

                                my $uri = URI->new;
                                $uri->query($query_str);

                                $query = $uri->query_form_hash;
                            }
                            else {
                                $query = $request->uri->query_form_hash;
                            }

                            if (!defined $query->{method}) {
                                $response->code(HTTP_BAD_REQUEST);
                                $response->content('No Method');
                            }
                            elsif ($query->{method} =~ /\W/o) {
                                $response->code(HTTP_BAD_REQUEST);
                                $response->content('Invalid Method');
                            }
                            elsif (!PollMonster::API->can($query->{method})) {
                                $response->code(HTTP_BAD_REQUEST);
                                $response->content('Unknown Method');
                            }
                            else {
                                my $params = {};

                                if (defined $query->{params}) {
                                    eval {
                                        $params = $json->decode($query->{params});
                                    };
                                    if ($@) {
                                        $response->code(HTTP_BAD_REQUEST);
                                        $response->content('Invalid Parameters');
                                    }
                                    elsif (ref($params) ne 'HASH') {
                                        $response->code(HTTP_BAD_REQUEST);
                                        $response->content('Invalid Parameters');
                                    }
                                }

                                unless ($response->code) {
                                    my $method = $query->{method};
                                    my ($errno, $errstr, $r) = PollMonster::API->instance->$method($params);

                                    if (defined $errno) {
                                        $response->code(HTTP_INTERNAL_SERVER_ERROR);
                                        if (defined $errstr) {
                                            $response->content($errno.' '.$errstr);
                                        }
                                        else {
                                            $response->content($errno);
                                        }
                                    }
                                    else {
                                        if (defined $r) {
                                            unless (ref($r)) {
                                                $r = [$r];
                                            }

                                            eval {
                                                $r = $json->encode($r);
                                            };
                                            if ($@) {
                                                $self->{logger}->error('json_encode failed for method call ', $method);
                                                $r = undef;
                                            }
                                        }

                                        $response->header('Content-Type' => 'application/json; charset=utf-8');
                                        $response->content((defined $r ? $r : ''));
                                    }
                                }
                            }

                            unless ($response->code) {
                                $response->code(HTTP_OK);
                            }

                            if ($response->code != HTTP_OK and !length($response->content)) {
                                $response->content($response->code.' '.HTTP::Status::status_message($response->code)."\r\n");
                            }

                            $response->header('Connection' => 'close');
                            $response->header('Content-Length' => length($response->content));
                            unless (defined $response->header('Content-Type')) {
                                $response->header('Content-Type' => 'text/html');
                            }

                            unless ($response->protocol) {
                                $response->protocol('HTTP/1.1');
                            }

                            if (PollMonster::DEBUG) {
                                $self->{logger}->debug("\n".
                                                       $response->protocol.' '.$response->code.' '.HTTP::Status::status_message($response->code)."\n".
                                                       $response->headers_as_string("\n").
                                                       "\n".
                                                       $response->content);
                            }

                            $handle->push_write($response->protocol.' '.$response->code.' '.HTTP::Status::status_message($response->code)."\r\n");
                            $handle->push_write($response->headers_as_string("\r\n"));
                            $handle->push_write("\r\n");
                            $handle->push_write($response->content);
                            $handle->push_shutdown;
                        }
                    }
                });

            $self->{client}->{$handle} = {
                rbuf => '',
                handle => $handle
            };
        }, sub {
            my (undef, $host, $port) = @_;

            PollMonster::INFO and $self->{logger}->info('JSON server ready at http://', $host, ':', $port);

            PollMonster::SRV_LISTEN;
        };
    }

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

sub close {
    my ($self) = @_;

    if (defined $self->{rpc}) {
        $self->{rpc}->close();
        $self->{rpc} = undef;
    }

    $self;
}

=head2 function2

=cut

sub call_info {
    my ($self, $rpc, $cli) = @_;
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
