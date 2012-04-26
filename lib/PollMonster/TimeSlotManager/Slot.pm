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

package PollMonster::TimeSlotManager::Slot;

use common::sense;

use PollMonster;

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
    my $self = {
        slot_size => 0,
        work_avg => 0,
        start => 0,
        @_
    };
    bless $self, $class;

    PollMonster::OBJ_DEBUG and Log::Log4perl->get_logger->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and Log::Log4perl->get_logger->debug('destroy ', __PACKAGE__, ' ', $_[0]);
}

sub uuid {
    $_[0]->{uuid};
}

sub set_uuid {
    $_[0]->{uuid} = $_[1] if (defined $_[1]);

    $_[0];
}

sub slot_size {
    $_[0]->{slot_size};
}

sub set_slot_size {
    $_[0]->{slot_size} = $_[1] if (defined $_[1]);

    $_[0];
}

sub work_avg {
    $_[0]->{work_avg};
}

sub set_work_avg {
    $_[0]->{work_avg} = $_[1] if (defined $_[1]);

    $_[0];
}

sub start {
    $_[0]->{start};
}

sub set_start {
    $_[0]->{start} = $_[1] if (defined $_[1]);

    $_[0];
}

sub start_hhmmss {
    strftime('%H:%M:%S', localtime($_[0]->{start}));
}

sub TO_JSON {
    [ $_[0]->{uuid}, $_[0]->{slot_size}, $_[0]->{work_avg}, $_[0]->{start} ];
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
