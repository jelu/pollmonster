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

package WorkerDB;

use common::sense;

sub _CONFIG (){ 'module worker-db' }

use PollMonster;
use PollMonster::ModuleFactory;

use WorkerDB::Work;
use WorkerDB::Result;

use WorkerDB::Backend::mysql;

my (@config, $dsn, $backend, $username, $password, $dbh, $prefix);

if (defined PollMonster->CFG(_CONFIG)) {
    unless (defined ($username = PollMonster->CFG(_CONFIG, 'username'))) {
        die 'username not configured';
    }

    unless (defined ($password = PollMonster->CFG(_CONFIG, 'password'))) {
        die 'password not configured';
    }

    $prefix = PollMonster->CFG(_CONFIG, 'prefix');
    unless (defined $prefix) {
        $prefix = 'pm_';
    }
    unless ($prefix =~ /^[a-zA-Z0-9_]*$/o) {
        die 'prefix "'.$prefix.'" is invalid';
    }

    if ($prefix ne '' and $prefix !~ /_$/o) {
        $prefix .= '_';
    }

    unless (defined ($dsn = PollMonster->CFG(_CONFIG, 'dsn'))) {
        die 'dsn not configured';
    }

    if ($dsn =~ /^[^:]+:([^:]+):/o) {
        my $backend_name = $1;

        if ($backend_name eq 'mysql') {
            $backend = WorkerDB::Backend::mysql->new(
                dsn => $dsn,
                username => $username,
                password => $password,
                prefix => $prefix
                );
        }
    }
    unless (defined $backend) {
        die 'Backend in '.$dsn.' not supported or connection error';
    }

    if (PollMonster->CFG(_CONFIG, 'install_or_upgrade')) {
        $backend->install_or_upgrade($prefix);
    }

    unless ($backend->verify_version) {
        die 'Backend not initialized or need upgrade';
    }

    $backend->fetch_types;
}

PollMonster::ModuleFactory->instance->register_worker(
    'db',
    { new => sub {
        return WorkerDB::Work->new(
            types => [],
            identifiers => [],
            values => [],
            ts => []
            );
      },
      verify => sub {
          my ($payload) = @_;
          
          if ($payload->isa('WorkerDB::Work')) {
              return 1;
          }

          return;
      },
      to_result => sub {
          my ($payload) = @_;

          return WorkerDB::Result->new;
      },
      run => sub {
          my ($payload) = @_;
          my ($id_href, @ids);
          my $retry;

          while () {
              unless (defined $backend) {
                  return;
              }

              eval {
                  unless ($backend->store_types($payload->[WorkerDB::Work::_TYPES])) {
                      return;
                  }

                  unless (($id_href = $backend->store_fetch_entities_id($payload->[WorkerDB::Work::_IDENTIFIERS]))) {
                      return;
                  }

                  foreach my $identifier (@{$payload->[WorkerDB::Work::_IDENTIFIERS]}) {
                      push(@ids, $id_href->{$identifier});
                  }

                  unless ($backend->store_entities_values(
                              $payload->[WorkerDB::Work::_TYPES],
                              \@ids,
                              $payload->[WorkerDB::Work::_VALUES],
                              $payload->[WorkerDB::Work::_TS]))
                  {
                      return;
                  }

                  $retry = 1;
              };

              if ($@) {
                  warn $@;
                  $backend = $backend->clone;
              }

              if (defined $retry) {
                  last;
              }
              $retry = 1;
          }

          return;
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
