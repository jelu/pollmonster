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

package WorkerSNMP::Get;

use common::sense;

use PollMonster;
use PollMonster::ModuleFactory;

use WorkerSNMP::Get::Work;
use WorkerSNMP::Get::Result;
use WorkerSNMP::XS qw(snmp_xs_get);

=head2 function1

=cut

PollMonster::ModuleFactory->instance->register_worker(
    'snmp_get',
    { new => sub {
        return WorkerSNMP::Get::Work->new(
            timeout => 10,
            retries => 3,
            hosts => [],
            oids => []
            );
      },
      verify => sub {
          my ($payload) = @_;

          if ($payload->isa('WorkerSNMP::Get::Work')) {
              return 1;
          }

          return;
      },
      to_result => sub {
          my ($payload) = @_;

          if (ref($payload) eq 'ARRAY' and @$payload == 3) {
              return WorkerSNMP::Get::Result->new(
                  oids => $payload->[WorkerSNMP::Get::Result::_OIDS],
                  result => $payload->[WorkerSNMP::Get::Result::_RESULT],
                  error => $payload->[WorkerSNMP::Get::Result::_ERROR]
                  );
          }

          return WorkerSNMP::Get::Result->new(
              oids => [],
              results => {},
              errors => {}
              );
      },
      run => sub {
          my ($payload) = @_;
          my (%result, %error);

          snmp_xs_get(
              $payload->[WorkerSNMP::Get::Work::_VERSION],
              $payload->[WorkerSNMP::Get::Work::_COMMUNITY], 
              $payload->[WorkerSNMP::Get::Work::_TIMEOUT],
              $payload->[WorkerSNMP::Get::Work::_RETRIES],
              $payload->[WorkerSNMP::Get::Work::_OIDS],
              $payload->[WorkerSNMP::Get::Work::_HOSTS],
              \%result,
              \%error);

          [ $payload->[WorkerSNMP::Get::Work::_OIDS], \%result, \%error ];
      }
    });

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
