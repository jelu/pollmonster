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

package PollMonster::ProcessManager::Helper;

use common::sense;
use Carp;

use PollMonster;
use PollMonster::ProcessManager::Client;

use AnyEvent ();

use POSIX qw(getpid);
use Scalar::Util qw(weaken);

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;
our $UNREGISTRATING;

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

sub register {
    my ($self, $uri, $callback, $heartbeat) = @_;

    unless (defined $self->{rpc}) {
        croak 'Can not register uninitialized '.ref($self);
    }

    unless (ref($callback) eq 'CODE') {
        $callback = undef;
    }

    unless (defined $heartbeat) {
        $heartbeat = 1;
    }

    $self->{registered} = 1;

    # LEAK pm_cli (add timer to undef)
    my $pm_cli; $pm_cli = PollMonster::ProcessManager::Client->new(
        uri => $uri,
        on_error => sub {
            $self->{registered} = 0;
            delete $self->{heartbeat_timer};

            if (defined $callback) {
                $callback->($self);
            }

            $pm_cli = undef;
        },
        on_connect => sub {
            my ($rpc, $name) = @_;

            $rpc->register($self->{rpc}->name, $self->{rpc}->uri, getpid);
        },
        service => {
            register => sub {
                my (undef, $answer) = @_;
                if (!ref($answer) and $answer == 1) {
                    $self->{registered} = 2;
                }

                if (defined $callback) {
                    $callback->($self);
                }

                $pm_cli = undef;
            }
        }
        );

    if ($heartbeat) {
        weaken($self);

        $self->{heartbeat_timer} = AnyEvent->timer(
            after => PollMonster::HEARTBEAT_INTERVAL,
            interval => PollMonster::HEARTBEAT_INTERVAL,
            cb => sub {
                if (defined $self and $self->is_registered) {
                    # LEAK pm_cli (add timer to undef)
                    my $pm_cli; $pm_cli = PollMonster::ProcessManager::Client->new(
                        uri => $uri,
                        on_error => sub {
                            $pm_cli = undef;
                        },
                        on_connect => sub {
                            my ($rpc, $name) = @_;
                            
                            if (defined $self) {
                                $rpc->heartbeat($self->{rpc}->name, $self->{rpc}->uri, getpid);
                            }
                            else {
                                $pm_cli = undef;
                            }
                        },
                        service => {
                            heartbeat => sub {
                                $pm_cli = undef;
                            }
                        }
                        );
                }
            });
    }

    return 1;
}

=head2 function1

=cut

sub is_registered {
    $_[0]->{registered};
}

=head2 function1

=cut

sub unregister {
    my ($self, $uri, $callback) = @_;

    unless (defined $self->{rpc}) {
        croak 'Can not unregister uninitialized '.ref($self);
    }

    unless (ref($callback) eq 'CODE') {
        $callback = undef;
    }

    if ($self->{registered}) {
        delete $self->{heartbeat_timer};
        $UNREGISTRATING++;

        my ($pm_cli, $rpc_name, $rpc_uri) = (undef, $self->{rpc}->name, $self->{rpc}->uri);

        weaken($self);

        # LEAK pm_cli (add timer to undef)
        $pm_cli = PollMonster::ProcessManager::Client->new(
            uri => $uri,
            on_error => sub {
                if (defined $self and defined $callback) {
                    $callback->($self);
                }

                $UNREGISTRATING--;
                $pm_cli = undef
            },
            on_connect => sub {
                my ($rpc, $name) = @_;

                $rpc->unregister($rpc_name, $rpc_uri, getpid);
            },
            service => {
                unregister => sub {
                    if (defined $self and defined $callback) {
                        $callback->($self);
                    }

                    $UNREGISTRATING--;
                    $pm_cli = undef;
                }
            }
            );
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
