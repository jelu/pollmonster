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

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

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

package WorkerDB::Work;

use common::sense;
use Carp;

use base qw(PollMonster::Payload::Work);

use PollMonster;

our $VERSION = $PollMonster::VERSION;

sub _TYPES          (){ 0 }
sub _IDENTIFIERS    (){ 1 }
sub _VALUES         (){ 2 }
sub _TS             (){ 3 }

sub module (){
    'db';
}

sub types {
    $_[0]->{types};
}

sub add_types {
    my $self = shift;

    foreach (@_) {
        push(@{$self->{types}}, $_) if (defined $_);
    }

    $self;
}

sub set_types {
    $_[0]->{types} = $_[1] if (defined $_[1]);

    $_[0];
}

sub push_entity {
    my ($self, $identifier, $values, $ts) = @_;

    unless (defined $self->{types}) {
        croak 'set types before pushing entities';
    }

    unless (ref($values) eq 'ARRAY') {
        croak 'values are not an array';
    }

    unless (@$values == @{$self->{types}}) {
        croak 'wrong number of values, does not match types';
    }

    push(@{$self->{identifiers}}, $identifier);
    push(@{$self->{values}}, $values);
    push(@{$self->{ts}}, $ts);

    $self;
}

sub TO_JSON {
    my ($self) = @_;

    [ $self->{types}, $self->{identifiers}, $self->{values}, $self->{ts} ];
}

sub is_valid {
    my ($self) = @_;

    @{$self->{types}} and @{$self->{identifiers}} and @{$self->{values}} and @{$self->{ts}};
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
