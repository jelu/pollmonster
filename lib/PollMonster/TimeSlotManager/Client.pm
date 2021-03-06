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

package PollMonster::TimeSlotManager::Client;

use common::sense;
use Carp;

use PollMonster qw(:name);
use PollMonster::RPC::Client;

use Log::Log4perl ();
use Scalar::Util qw(weaken);
use POSIX qw(strftime);

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
        logger => Log::Log4perl->get_logger
    };
    bless $self, $class;
    my $rself = $self;
    weaken($self);

    if (exists $args{on_connect} and ref($args{on_connect}) eq 'CODE') {
        $self->{on_connect} = $args{on_connect};
    }
    if (exists $args{on_error} and ref($args{on_error}) eq 'CODE') {
        $self->{on_error} = $args{on_error};
    }
    if (exists $args{on_eof} and ref($args{on_eof}) eq 'CODE') {
        $self->{on_eof} = $args{on_eof};
        $args{on_eof} = sub {
            $self->close;
            $self->{on_eof}->($self);
        };
    }
    if (exists $args{on_noauth} and ref($args{on_noauth}) eq 'CODE') {
        $self->{on_noauth} = $args{on_noauth};
        $args{on_noauth} = sub {
            $self->close;
            $self->{on_noauth}->($self);
        };
    }

    $args{on_connect} = sub {
        my ($rpc, $name) = @_;

        unless (defined $self->{rpc} and $self->{rpc}->is_connected) {
            if (exists $self->{on_error}) {
                $self->{on_error}->($self, 'connected but lost rpc object');
            }
            return;
        }

        unless ($name eq TIMESLOT_MANAGER_NAME) {
            if (exists $self->{on_error}) {
                $self->{on_error}->($self, 'connected to wrong service, expected '.TIMESLOT_MANAGER_NAME.' but got '.$name);
            }
            return;
        }

        if (exists $self->{on_connect}) {
            $self->{on_connect}->($self, $name);
        }
    };
    $args{on_error} = sub {
        my ($rpc, $message) = @_;

        $self->close;

        if (exists $self->{on_error}) {
            $self->{on_error}->($self, $message);
        }
    };

    unless (exists $args{service}) {
        $args{service} = {};
    }

    foreach my $callback (qw(allocate)) {
        unless (exists $args{service}->{$callback}) {
            $args{service}->{$callback} = sub { croak 'Uncatch callback ('.$callback.') in '.__PACKAGE__ };
        }
    }

    $self->{rpc} = PollMonster::RPC::Client->new(%args);

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and $_[0]->{logger}->debug('destroy ', __PACKAGE__, ' ', $_[0]);
}

=head2 function2

=cut

sub is_connected {
    defined $_[0]->{rpc} and $_[0]->{rpc}->is_connected;
}

=head2 function2

=cut

sub uri {
    defined $_[0]->{rpc} and $_[0]->{rpc}->uri;
}

=head2 function2

=cut

sub close {
    $_[0]->{rpc} = undef;

    $_[0];
}

=head2 function2

=cut

sub allocate {
    my ($self, $uuid, $slot_size, $work_avg, $after) = @_;

    unless (defined $uuid and defined $slot_size and defined $work_avg) {
        Carp::croak 'missing parameters';
    }

    unless (defined $self->{rpc}) {
        return;
    }

    return $self->{rpc}->call('allocate', $uuid, $slot_size, $work_avg, $after);
}

=head2 function2

=cut

sub timeslots {
    my ($self) = @_;

    unless (defined $self->{rpc}) {
        return;
    }

    return $self->{rpc}->call('timeslots');
}

=head2 function2

=cut

sub timeslots_to_string {
    my ($self, $timeslots_array) = @_;
    my ($load, $output) = (0, '');

    if (ref($timeslots_array) eq 'ARRAY') {
        foreach my $timeslots (@$timeslots_array)
        {
            unless (ref($timeslots) eq 'ARRAY') {
                next;
            }

            $output .= 'Timeslots '.$load.":\n";

            foreach my $slot (@$timeslots)
            {
                unless (ref($slot) eq 'ARRAY') {
                    next;
                }

                $output .= 'uuid: '.$slot->[0].' start: '.$slot->[3].' / '.strftime('%H:%M:%S', localtime($slot->[3])).' work_avg: '.$slot->[2]."\n";
            }
            $load++;
        }
    }

    return $output;
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
