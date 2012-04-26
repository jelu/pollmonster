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

package WorkerDBQuery;

use common::sense;

sub _CONFIG (){ 'module worker-db-query' }

use PollMonster;
use PollMonster::ModuleFactory;

use WorkerDBQuery::Work;
use WorkerDBQuery::Result;

use DBI;

PollMonster::ModuleFactory->instance->register_worker(
    'db-query',
    { new => sub {
        return WorkerDBQuery::Work->new();
      },
      verify => sub {
          my ($payload) = @_;
          
          if ($payload->isa('WorkerDBQuery::Work')) {
              return 1;
          }

          return;
      },
      to_result => sub {
          my ($payload) = @_;

          if (ref($payload) eq 'ARRAY' and @$payload == 2) {
              return WorkerDBQuery::Result->new(
                  result => $payload->[WorkerDBQuery::Result::_RESULT],
                  error => $payload->[WorkerDBQuery::Result::_ERROR]
                  );
          }

          return WorkerDBQuery::Result->new(
              result => []
              );
      },
      run => sub {
          my ($payload) = @_;
          my ($dsn, $username, $password, $dbh, $sth, @result);

          if (defined $payload->[WorkerDBQuery::Work::_USERNAME]) {
              $username = $payload->[WorkerDBQuery::Work::_USERNAME];
          }

          if (defined $payload->[WorkerDBQuery::Work::_PASSWORD]) {
              $password = $payload->[WorkerDBQuery::Work::_PASSWORD];
          }

          if (defined ($dsn = PollMonster->CFG(_CONFIG, $payload->[WorkerDBQuery::Work::_DSN]))) {
              unless (defined $username) {
                  $username = PollMonster->CFG(_CONFIG, $payload->[WorkerDBQuery::Work::_DSN].'-username');
              }
              unless (defined $password) {
                  $password = PollMonster->CFG(_CONFIG, $payload->[WorkerDBQuery::Work::_DSN].'-password');
              }
          }
          else {
              $dsn = $payload->[WorkerDBQuery::Work::_DSN];
          }

          unless ($dsn =~ /^DBI:/oi) {
              return [ undef, 'Invalid DSN: '.$dsn ];
          }

          eval {
              $dbh = DBI->connect($dsn, $username, $password);
          };

          if ($@ or !defined $dbh) {
              return [ undef, $@ ? $@ : $DBI::errstr ];
          }

          $dbh->{mysql_enable_utf8} = 1;
          $dbh->{RaiseError} = 0;
          $dbh->{PrintError} = 0;
          
          unless (defined ($sth = $dbh->prepare($payload->[WorkerDBQuery::Work::_SQL]))) {
              my $errstr = $dbh->errstr;
              $dbh->disconnect;
              return [ undef, $errstr ];
          }

          unless ($sth->execute) {
              my $errstr = $dbh->errstr;
              $dbh->disconnect;
              return [ undef, $errstr ];
          }

          while (defined (my $row = $sth->fetchrow_hashref)) {
              push(@result, $row);
          }
          $sth->finish;
          $dbh->disconnect;

          return [ \@result, undef ];
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
