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

package PollMonster::WorkDispatcher::Worker;

use common::sense;

use PollMonster;

use Log::Log4perl ();

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
    my $self = {
        @_
    };
    bless $self, $class;

    PollMonster::OBJ_DEBUG and Log::Log4perl->get_logger->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and Log::Log4perl->get_logger->debug('destroy ', __PACKAGE__, ' ', $_[0]);
}

sub uri {
    $_[0]->{uri};
}

sub set_uri {
    $_[0]->{uri} = $_[1] if (defined $_[1]);

    $_[0];
}

sub rpc {
    $_[0]->{rpc};
}

sub set_rpc {
    $_[0]->{rpc} = $_[1] if (defined $_[1]);

    $_[0];
}

sub work {
    $_[0]->{work};
}

sub set_work {
    $_[0]->{work} = $_[1] if (defined $_[1]);

    $_[0];
}

sub del_work {
    delete $_[0]->{work};
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
