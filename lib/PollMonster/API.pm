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

package PollMonster::API;

use common::sense;
use Carp;

use PollMonster qw(:name);
use PollMonster::ProcessManager::Client;
use PollMonster::ProcessorCallback::Server;

use Coro ();
use Coro::Signal ();
use Coro::Timer ();

use Log::Log4perl ();

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;
our $INSTANCE;

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
    my $self = {
        logger => Log::Log4perl->get_logger
    };
    bless $self, $class;

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and $_[0]->{logger}->debug('destroy ', __PACKAGE__, ' ', $_[0]);
}

=head2 function1

=cut

sub instance {
    $INSTANCE ||= PollMonster::API->new();
}

=head2 function1

=cut

sub ping {
    return undef, undef, 'pong';
}

=head2 function1

=cut

sub list {
    my ($self, $args) = @_;

    unless (ref($args) eq 'HASH') {
        return -1, 'Wrong arguments';
    }

    my @list;
    my $sig = Coro::Signal->new;
    my $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            if (defined $args->{type}) {
                shift->list($args->{type});
            }
            else {
                shift->list;
            }
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

    return undef, undef, \@list;
}

=head2 function2

=cut

sub add_order_callback {
    my ($self, $args) = @_;

    unless (ref($args) eq 'HASH') {
        return -1, 'Wrong arguments';
    }

    my (@list, $uuid, @result, @ret);
    my $sig = Coro::Signal->new;
    my $cb_sig = Coro::Signal->new;

    foreach (qw(module parameter)) {
        unless (exists $args->{$_}) {
            return -1, 'Wrong arguments';
        }
    }

    unless (ref($args->{parameter}) eq 'HASH') {
        return -1, 'Wrong arguments';
    }

    my $pm = PollMonster::ProcessManager::Client->new(
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

    unless (@list) {
        return -2, 'No Order Manager found';
    }

    @ret = (undef, undef);

    my $pc_rpc = PollMonster::ProcessorCallback::Server->new(
        uri => PollMonster->CFG(PROCESSOR_CALLBACK_NAME, 'uri'),
        on_result => sub {
            my (undef, $result, $done) = @_;

            if (defined $result) {
                push @result, $result;
            }

            if ($done) {
                $cb_sig->send;
            }
        },
        on_error => sub {
            my (undef, $errno, $errstr) = @_;

            $ret[0] = $errno;
            $ret[1] = $errstr;

            $cb_sig->send;
        }
        );

    foreach my $om (@list) {
        my $om = PollMonster::OrderManager::Client->new(
            uri => $om->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->add($args->{module}, $args->{parameter}, { callback => $pc_rpc->uri, push => 1 });
            },
            service => {
                add => sub {
                    my (undef, $id) = @_;
                    if (defined $id) {
                        $uuid = $id;
                        @list = ();
                    }
                    $sig->send;
                }
            }
            );
        $sig->wait;

        last if (defined $uuid);
    }

    if ($uuid) {
        $cb_sig->wait;

        return @ret, \@result;
    }

    return -3, 'No order uuid returned';
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
