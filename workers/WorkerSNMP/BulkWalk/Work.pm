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

package WorkerSNMP::BulkWalk::Work;

use common::sense;

use base qw(PollMonster::Payload::Work);

use PollMonster;

our $VERSION = $PollMonster::VERSION;

sub _VERSION         (){ 0 }
sub _COMMUNITY       (){ 1 }
sub _TIMEOUT         (){ 2 }
sub _RETRIES         (){ 3 }
sub _NON_REPEATERS   (){ 4 }
sub _MAX_REPETITIONS (){ 5 }
sub _BASE_OID        (){ 6 }
sub _OID             (){ 7 }
sub _HOSTS           (){ 8 }

sub module (){
    'snmp_bulkwalk';
}

sub version {
    $_[0]->{version};
}

sub set_version {
    $_[0]->{version} = $_[1] if (defined $_[1]);

    $_[0];
}

sub community {
    $_[0]->{community};
}

sub set_community {
    $_[0]->{community} = $_[1] if (defined $_[1]);

    $_[0];
}

sub timeout {
    $_[0]->{timeout};
}

sub set_timeout {
    $_[0]->{timeout} = $_[1] if (defined $_[1]);

    $_[0];
}

sub retries {
    $_[0]->{retries};
}

sub set_retries {
    $_[0]->{retries} = $_[1] if (defined $_[1]);

    $_[0];
}

sub non_repeaters {
    $_[0]->{non_repeaters};
}

sub set_non_repeaters {
    $_[0]->{non_repeaters} = $_[1] if (defined $_[1]);

    $_[0];
}

sub max_repetitions {
    $_[0]->{max_repetitions};
}

sub set_max_repetitions {
    $_[0]->{max_repetitions} = $_[1] if (defined $_[1]);

    $_[0];
}

sub base_oid {
    $_[0]->{base_oid};
}

sub set_base_oid {
    if (defined $_[1]) {
        $_[0]->{base_oid} = $_[1];

        unless (defined $_[0]->{oid}) {
            $_[0]->{oid} = $_[1];
        }
    }

    $_[0];
}

sub oid {
    $_[0]->{oid};
}

sub set_oid {
    $_[0]->{oid} = $_[1] if (defined $_[1]);

    $_[0];
}

sub hosts {
    $_[0]->{hosts};
}

sub add_hosts {
    my $self = shift;

    foreach (@_) {
        push(@{$self->{hosts}}, $_) if (defined $_);
    }

    $self;
}

sub set_hosts {
    $_[0]->{hosts} = $_[1] if (defined $_[1]);

    $_[0];
}

sub TO_JSON {
    my ($self) = @_;

    [ $self->{version},
      $self->{community},
      $self->{timeout},
      $self->{retries},
      $self->{non_repeaters},
      $self->{max_repetitions},
      $self->{base_oid},
      $self->{oid},
      $self->{hosts}
    ];
}

sub is_valid {
    my ($self) = @_;

    defined $self->{version} and defined $self->{community} and defined $self->{base_oid} and defined $self->{oid} and @{$self->{hosts}};
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
