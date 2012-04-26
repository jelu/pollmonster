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

package WorkerDB::Backend::mysql;

use common::sense;

use base qw(WorkerDB::Backend);

our $VERSION = 1;

sub _CACHE_TIME (){ 3600 }
sub _CACHE_RETRY_TIME (){ 60 }

use PollMonster;

use DBI ();
use DBD::mysql ();

use Log::Log4perl ();

=head2 function1

=cut

my %_tbl = (
    workerdb_snmp => q{
create table %sworkerdb (
  version int unsigned not null
) engine innodb charset utf8 },
    entities => q{
create table %sentities (
  entity_id int unsigned not null auto_increment,
  entity_identifier varchar(64) not null,
  primary key (entity_id),
  unique (entity_identifier)
) engine innodb charset utf8 },
    entity_values => q{
create table %sentity_values (
  entity_id int unsigned not null,
  type_id int unsigned not null,
  index_value int unsigned not null,
  numeric_value bigint unsigned null,
  string_value varchar(255) null,
  value_ts int unsigned not null,
  primary key (entity_id, type_id, index_value),
  key (type_id, index_value),
  key (numeric_value),
  key (string_value(64)),
  key (value_ts)
) engine innodb charset utf8 },
    types => q{
create table %stypes (
  type_id int unsigned not null auto_increment,
  type_identifier varchar(255) not null,
  primary key (type_id),
  unique (type_identifier)
) engine innodb charset utf8 }
    );

=head2 function1

=cut

my @_def_data =
    ( q{insert into %sworkerdb values ( 1 )},
    );

=head2 function1

=cut

sub new {
    my $this = shift;
    my $class = ref($this) || $this;
    my $self = {
        logger => Log::Log4perl->get_logger,
        ctime_types => 0,
        cache_types => {},
        @_
    };
    bless $self, $class;

    $self->{dbh} = DBI->connect($self->{dsn}, $self->{username}, $self->{password})
        or die $DBI::errstr;

    $self->{dbh}->{mysql_enable_utf8} = 1;
    $self->{dbh}->{mysql_auto_reconnect} = 1;
    $self->{dbh}->{RaiseError} = 0;
    $self->{dbh}->{PrintError} = 0;

    $self->{sth_fetch_version} = $self->{dbh}->prepare('SELECT version FROM '.$self->{prefix}.'workerdb') and
        $self->{sth_fetch_types} = $self->{dbh}->prepare('SELECT type_id, type_identifier FROM '.$self->{prefix}.'types') and
        $self->{sth_store_type} = $self->{dbh}->prepare('INSERT IGNORE INTO '.$self->{prefix}.'types (type_identifier) VALUES (?)') and
        $self->{sth_fetch_entity} = $self->{dbh}->prepare('SELECT entity_id FROM '.$self->{prefix}.'entities WHERE entity_identifier = ?') and
        $self->{sth_fetch_entities_id} = $self->{dbh}->prepare('SELECT entity_id, entity_identifier FROM '.$self->{prefix}.'entities WHERE entity_identifier IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)') and
        $self->{sth_store_entity} = $self->{dbh}->prepare('INSERT INTO '.$self->{prefix}.'entities (entity_identifier) VALUES (?)') and
        $self->{sth_store_ignore_entity} = $self->{dbh}->prepare('INSERT IGNORE INTO '.$self->{prefix}.'entities (entity_identifier) VALUES (?)') and
        $self->{sth_store_entity_values} = [
            'INSERT INTO '.$self->{prefix}.'entity_values (entity_id,type_id,index_value,numeric_value,string_value,value_ts) VALUES ',
            ' ON DUPLICATE KEY UPDATE numeric_value=VALUES(numeric_value), string_value=VALUES(string_value), value_ts=VALUES(value_ts)' ]
            or die $self->{dbh}->errstr;

    PollMonster::OBJ_DEBUG and Log::Log4perl->get_logger->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    my ($self) = @_;

    PollMonster::OBJ_DEBUG and Log::Log4perl->get_logger->debug('destroy ', __PACKAGE__, ' ', $self);

    if (defined $self->{dbh}) {
        $self->{dbh}->disconnect;
    }
}

=head2 function1

=cut

sub clone {
    my ($self) = @_;

    $self->new(
        dsn => $self->{dsn},
        username => $self->{username},
        password => $self->{password},
        prefix => $self->{prefix}
        );
}

=head2 function1

=cut

sub verify_version {
    my ($self) = @_;

    if ($self->{sth_fetch_version}->execute) {
        my ($version) = $self->{sth_fetch_version}->fetchrow_array;
        $self->{sth_fetch_version}->finish;

        if (defined $version and $version == $VERSION) {
            return 1;
        }
    }

    return;
}

=head2 function1

=cut

sub install_or_upgrade {
    my ($self, $prefix) = @_;

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    unless ($self->{sth_fetch_version}->execute()) {
        $self->{dbh}->begin_work;
        eval {
            foreach my $sql (values %_tbl) {
                $self->{dbh}->do(sprintf($sql, $prefix));
            }
            foreach my $sql (@_def_data) {
                $self->{dbh}->do(sprintf($sql, $prefix));
            }
            $self->{dbh}->commit;
        };
        if ($@) {
            $self->{dbh}->rollback;
            die $@;
        }
    }
    else {
        my ($version) = $self->{sth_fetch_version}->fetchrow_array;
        $self->{sth_fetch_version}->finish;

        unless (defined $version) {
            die 'Unable to get version of database tables';
        }

        # upgrade stuff
    }

    $self;
}

=head2 function1

=cut

sub fetch_types {
    my ($self, $force) = @_;

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    if ($self->{ctime_types} < time or defined $force) {
        unless ($self->{sth_fetch_types}->execute()) {
            $self->{logger}->warn('Unable to cache types: '.$self->{dbh}->errstr);
            $self->{ctime_types} = time + _CACHE_RETRY_TIME;
        }
        else {
            $self->{cache_types} = {};
            while ((my $href = $self->{sth_fetch_types}->fetchrow_hashref)) {
                $self->{cache_types}->{$href->{type_identifier}} = $href->{type_id};
            }
            $self->{sth_fetch_types}->finish;
            $self->{ctime_types} = time + _CACHE_TIME;
        }
    }

    return 1;
}

=head2 function1

=cut

sub store_types {
    my ($self, $types) = @_;
    my $force = 0;

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    $self->fetch_types;

    foreach my $type (@$types) {
        unless (exists $self->{cache_types}->{$type}) {
            unless ($self->{sth_store_type}->execute($type)) {
                $self->{logger}->warn($self->{dbh}->errstr);
                return;
            }
            $self->{sth_store_type}->finish;
            
            $force = 1;
        }
    }

    $self->fetch_types($force);

    foreach my $type (@$types) {
        unless (exists $self->{cache_types}->{$type}) {
            $self->{logger}->warn('store_types failed to store some types');
            return;
        }
    }

    return 1;
}

=head2 function1

=cut

sub fetch_entities_id {
    my ($self, $identifiers, $entities_href) = @_;
    my (%entities, @identifiers);

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    %entities = ();

    unless (defined $entities_href) {
        $entities_href = \%entities;
    }

    foreach my $identifier (@$identifiers) {
        push(@identifiers, $identifier);

        if (@identifiers == 10) {
            if ($self->{sth_fetch_entities_id}->execute(@identifiers)) {
                while ((my ($entity_id, $entity_identifier) = $self->{sth_fetch_entities_id}->fetchrow_array)) {
                    $entities_href->{$entity_identifier} = $entity_id;
                }
                $self->{sth_fetch_entities_id}->finish;
            }

            @identifiers = ();
        }
    }

    if (@identifiers) {
        for ((scalar @identifiers + 1) .. 10) {
            push(@identifiers, '');
        }

        if ($self->{sth_fetch_entities_id}->execute(@identifiers)) {
            while ((my ($entity_id, $entity_identifier) = $self->{sth_fetch_entities_id}->fetchrow_array)) {
                $entities_href->{$entity_identifier} = $entity_id;
            }
            $self->{sth_fetch_entities_id}->finish;
        }
    }
    
    $entities_href;
}

=head2 function1

=cut

sub store_entity {
    my ($self, $identifier) = @_;

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    unless ($self->{sth_store_entity}->execute($identifier)) {
        my $err = $self->{dbh}->errstr;

        if ($self->{sth_fetch_entity}->execute($identifier)) {
            if ((my ($entity_id) = $self->{sth_fetch_entity}->fetchrow_array)) {
                $self->{sth_fetch_entity}->finish;
                return $entity_id;
            }
            $self->{sth_fetch_entity}->finish;
        }

        $self->{logger}->warn($err);
        return;
    }
    $self->{sth_store_entity}->finish;

    return $self->{dbh}->last_insert_id(undef,undef,undef,undef);
}

=head2 function1

=cut

sub store_fetch_entities_id {
    my ($self, $identifiers) = @_;
    my (%entities, @missing);

    %entities = ();
    
    $self->fetch_entities_id($identifiers, \%entities);

    foreach my $identifier (@$identifiers) {
        unless (exists $entities{$identifier}) {
            push(@missing, $identifier);
        }
    }

    if (@missing) {
        foreach my $identifier (@missing) {
            unless ($self->{sth_store_ignore_entity}->execute($identifier)) {
                $self->{logger}->warn($self->{dbh}->errstr);
                $self->rollback;
                return;
            }
        }

        $self->fetch_entities_id(\@missing, \%entities);

        foreach my $identifier (@missing) {
            unless (exists $entities{$identifier}) {
                $self->{logger}->warn('unable to store and fetch all identifiers');
                return;
            }
        }
    }

    \%entities;
}

=head2 function1

=cut

sub store_entity_values {
    my ($self, $types, $entity_id, $values, $ts) = @_;
    my (@sqls, $i);

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    $self->begin_work;

    $i = 0;
    foreach my $value (@$values) {
        if (defined $value and exists $self->{cache_types}->{$types->[$i]}) {
            if (ref($value) eq 'HASH') {
                while (my ($idx, $val) = each %$value) {
                    if (Scalar::Util::looks_like_number($val)) {
                        push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, $idx, int($val), 'null', $ts));
                    }
                    else {
                        push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, $idx, 'null', $self->{dbh}->quote($val), $ts));
                    }
                }
            }
            elsif (Scalar::Util::looks_like_number($value)) {
                push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, 0, int($value), 'null', $ts));
            }
            else {
                push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, 0, 'null', $self->{dbh}->quote($value), $ts));
            }
        }
        $i++;

        if (@sqls == 100) {
            unless ($self->{dbh}->do($self->{sth_store_entity_values}->[0].'('.join('),(',@sqls).')'.$self->{sth_store_entity_values}->[1])) {
                $self->{logger}->warn($self->{dbh}->errstr);
                $self->rollback;
                return;
            }

            @sqls = ();
        }
    }

    if (@sqls) {
        unless ($self->{dbh}->do($self->{sth_store_entity_values}->[0].'('.join('),(',@sqls).')'.$self->{sth_store_entity_values}->[1])) {
            $self->{logger}->warn($self->{dbh}->errstr);
            $self->rollback;
            return;
        }
    }

    $self->commit;

    return 1;
}

=head2 function1

=cut

sub store_entities_values {
    my ($self, $types, $entities_id, $values, $ts) = @_;
    my (@sqls, $ei, $i);

    unless (defined $self->{dbh}) {
        die 'not connected';
    }

    $self->begin_work;

    $ei = 0;
    foreach my $entity_id (@$entities_id) {
        unless (exists $ts->[$ei]) {
            last;
        }
        $i = 0;
        foreach my $value (@{$values->[$ei]}) {
            if (defined $value and exists $self->{cache_types}->{$types->[$i]}) {
                if (ref($value) eq 'HASH') {
                    while (my ($idx, $val) = each %$value) {
                        if (Scalar::Util::looks_like_number($val)) {
                            push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, $idx, int($val), 'null', $ts->[$ei]));
                        }
                        else {
                            push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, $idx, 'null', $self->{dbh}->quote($val), $ts->[$ei]));
                        }
                    }
                }
                elsif (Scalar::Util::looks_like_number($value)) {
                    push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, 0, int($value), 'null', $ts->[$ei]));
                }
                else {
                    push(@sqls, join(',', $entity_id, $self->{cache_types}->{$types->[$i]}, 0, 'null', $self->{dbh}->quote($value), $ts->[$ei]));
                }
            }
            $i++;

            if (@sqls == 100) {
                unless ($self->{dbh}->do($self->{sth_store_entity_values}->[0].'('.join('),(',@sqls).')'.$self->{sth_store_entity_values}->[1])) {
                    $self->{logger}->warn($self->{dbh}->errstr);
                    $self->rollback;
                    return;
                }

                @sqls = ();
            }
        }
        $ei++;
    }

    if (@sqls) {
        unless ($self->{dbh}->do($self->{sth_store_entity_values}->[0].'('.join('),(',@sqls).')'.$self->{sth_store_entity_values}->[1])) {
            $self->{logger}->warn($self->{dbh}->errstr);
            $self->rollback;
            return;
        }
    }

    $self->commit;

    return 1;
}

=head2 function1

=cut

sub begin_work {
    unless (defined $_[0]->{dbh}) {
        die 'not connected';
    }

    $_[0]->{dbh}->begin_work;
}

=head2 function1

=cut

sub commit {
    unless (defined $_[0]->{dbh}) {
        die 'not connected';
    }

    $_[0]->{dbh}->commit;
}

=head2 function1

=cut

sub rollback {
    unless (defined $_[0]->{dbh}) {
        die 'not connected';
    }

    $_[0]->{dbh}->rollback;
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
