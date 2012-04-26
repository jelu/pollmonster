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

package PollMonster::TimeSlotManager::Server;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Network;
use PollMonster::ProcessManager::Client;
use PollMonster::TimeSlotManager::Slot;

use Coro::RWLock ();

use Log::Log4perl ();
use Time::HiRes ();
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
        logger => Log::Log4perl->get_logger,
        registered => 0,
        timeslots => [],
        load => 0,
        lock => Coro::RWLock->new
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
    #$args{on_net_join} = sub { $self->on_net_join(@_); };

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(allocate timeslots)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{allocate} = sub { $self->call_allocate(@_) };
    $args{service}->{timeslots} = sub { $self->call_timeslots(@_) };

    $args{name} = TIMESLOT_MANAGER_NAME;
    $self->{rpc} = PollMonster::RPC::Network->new(%args);

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
        $self->{rpc} = undef;
    }

    $self;
}

=head2 function2

=cut

sub call_allocate {
    my ($self, $rpc, $cli, $uuid, $slot_size, $work_avg, $after) = @_;
    my ($time, $ok, $new_timeslots, $new_timeslot, $old_timeslot, $slot, $start, $new_slot);

    $time = Time::HiRes::time;
    unless (defined $after) {
        $after = $time;
    }
    if ($after < $time) {
        $after = $time;
    }
    $new_timeslots = [];
    $ok = 0;
    $self->{lock}->wrlock;

    while (($old_timeslot = shift(@{$self->{timeslots}}))) {
        $start = $after;
        $ok = 0;
        $new_timeslot = [];

        while (($slot = shift(@$old_timeslot))) {
            # if the timeslot is old skip it
            if (($slot->start + $slot->work_avg) < $time) {
                PollMonster::DEBUG and $self->{logger}->debug('removing old slot ', $slot->uuid, '/', $slot->start_hhmmss);
                next;
            }

            # if slot ends after we want to start we check it
            if (($slot->start + $slot->work_avg) > $after) {
                # if the slot starts before our end we check with our end
                if ($slot->start > ($after + $slot_size)) {
                    # if the work can fit between last work and our end we put it there
                    if (($after + $slot_size - $start) > $work_avg) {
                        PollMonster::DEBUG and $self->{logger}->debug('found slot after ', $slot->uuid, '/', $slot->start_hhmmss);
                        $new_slot = PollMonster::TimeSlotManager::Slot->new(
                            uuid => $uuid,
                            slot_size => $slot_size,
                            work_avg => $work_avg,
                            start => $start
                            );
                        push(@$new_timeslot, $new_slot);
                        $ok = 1;
                    }
                }
                else {
                    # if the work can fit between last work and when next start we put it there
                    if (($slot->start - $start) > $work_avg) {
                        PollMonster::DEBUG and $self->{logger}->debug('found slot after ', $slot->uuid, '/', $slot->start_hhmmss);
                        $new_slot = PollMonster::TimeSlotManager::Slot->new(
                            uuid => $uuid,
                            slot_size => $slot_size,
                            work_avg => $work_avg,
                            start => $start
                            );
                        push(@$new_timeslot, $new_slot);
                        $ok = 1;
                    }
                }

                $start = $slot->start + $slot->work_avg;
            }

            unless ($ok) {
                # if we passed the point where it will not work in this load we move to the next
                if ($start > ($after + $slot_size - $work_avg)) {
                    $ok = -1;
                }
            }

            push(@$new_timeslot, $slot);

            if ($ok) {
                last;
            }
        }

        if (@$old_timeslot) {
            push(@$new_timeslot, @$old_timeslot);
        }

        unless ($ok) {
            # this happens when the load it empty or if there is room after slots
            PollMonster::DEBUG and $self->{logger}->debug('found slot at end or empty load');
            $new_slot = PollMonster::TimeSlotManager::Slot->new(
                uuid => $uuid,
                slot_size => $slot_size,
                work_avg => $work_avg,
                start => $start
                );
            push(@$new_timeslot, $new_slot);
            $ok = 1;
        }

        if (@$new_timeslot) {
            push(@$new_timeslots, $new_timeslot);
        }

        unless ($ok == -1) {
            last;
        }
    }

    unless ($ok == 1) {
        # no room in any existing timeslot, create a new one
        PollMonster::DEBUG and $self->{logger}->debug('no room, made new load');
        $new_timeslot = [];
        $new_slot = PollMonster::TimeSlotManager::Slot->new(
            uuid => $uuid,
            slot_size => $slot_size,
            work_avg => $work_avg,
            start => $after
            );
        push(@$new_timeslot, $new_slot);
        push(@$new_timeslots, $new_timeslot);
    }

    while (($old_timeslot = shift(@{$self->{timeslots}}))) {
        $new_timeslot = [];

        while (($slot = shift(@$old_timeslot))) {
            # if the timeslot is old skip it
            if (($slot->start + $slot->work_avg) < $time) {
                PollMonster::DEBUG and $self->{logger}->debug('removing old slot ', $slot->uuid, '/', $slot->start_hhmmss);
                next;
            }

            push(@$new_timeslot, $slot);
        }

        if (@$new_timeslot) {
            push(@$new_timeslots, $new_timeslot);
        }
    }

    $self->{timeslots} = $new_timeslots;
    $self->{load} = @$new_timeslots;
    $self->{lock}->unlock;

    if (PollMonster::DEBUG) {
        my $load = 0;
        foreach my $timeslots (@$new_timeslots)
        {
            $self->{logger}->debug('Timeslots ', $load++);

            foreach my $slot (@$timeslots)
            {
                $self->{logger}->debug(
                    'uuid: ', $slot->uuid,
                    ' start: ', $slot->start, ' / ', $slot->start_hhmmss,
                    ' work_avg: ', $slot->work_avg
                    );
            }
        }
    }

    $rpc->call($cli, 'allocate', defined $new_slot ? $new_slot->start : undef);
    return;
}

=head2 function2

=cut

sub call_timeslots {
    my ($self, $rpc, $cli) = @_;

    $rpc->call($cli, 'timeslots', $self->{timeslots});
    return;
}

=head2 function2

=cut

sub on_net_join {
    my ($self, $rpc, $cli, $uri) = @_;

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
