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

package PollMonster::ModuleFactory;

use common::sense;
use Carp;
use Carp::Heavy;

use PollMonster;

use Log::Log4perl ();
use Cwd qw(getcwd);

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;
our $INSTANCE;

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
        logger => Log::Log4perl->get_logger,
        processor => {},
        worker => {},
        file => {},
        current_filename => undef,
        reload_processor => {},
        reload_worker => {}
    };
    bless $self, $class;

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and $_[0]->{logger}->debug('destroy ', __PACKAGE__, ' ', $_[0]);
}

=head2 function1

=cut

sub instance {
    $INSTANCE ||= PollMonster::ModuleFactory->new();
}

=head2 function1

=cut

sub load {
    my ($self, $entry) = @_;

    if (-d $entry) {
        unless (opendir(DIR, $entry)) {
            $self->{logger}->error('Unable to load modules from path: ', $entry);
            return $self;
        }
    
        $entry =~ s/\/*$//o;

        push(@INC, $entry);

        while ((my $ent = readdir(DIR))) {
            my $path = $entry.'/'.$ent;
            
            next if ($ent =~ /^\./o or $ent !~ /\.pm$/o or ! -f $path);

            $self->{current_filename} = $path;
            my $cwd = getcwd;
            unless (my $ret = do $path) {
                $self->{logger}->warn("couldn't parse $path: $@") if $@;
                $self->{logger}->warn("couldn't do $path: $!")    unless defined $ret;
                $self->{logger}->warn("couldn't run $path")       unless $ret;
            }
            else {
                $self->{logger}->info('loaded module file ',$path);
            }
            chdir($cwd);
        }
        close(DIR);
    }
    elsif (-f $entry) {
        $self->{current_filename} = $entry;
        my $cwd = getcwd;
        unless (my $ret = do $entry) {
            $self->{logger}->warn("couldn't parse $entry: $@") if $@;
            $self->{logger}->warn("couldn't do $entry: $!")    unless defined $ret;
            $self->{logger}->warn("couldn't run $entry")       unless $ret;
        }
        else {
            $self->{logger}->info('loaded module file ',$entry);
        }
        chdir($cwd);
    }

    $self;
}

=head2 function1

=cut

sub unload_processor {
    my ($self, $module) = @_;

    if (exists $self->{processor}->{$module}) {
        my $processor = delete $self->{processor}->{$module};
        $self->{file}->{$processor->{filename}}--;

        if ($self->{file}->{$processor->{filename}} < 1) {
            delete $INC{$processor->{filename}};
        }
    }

    $self;
}

=head2 function1

=cut

sub unload_worker {
    my ($self, $module) = @_;

    if (exists $self->{worker}->{$module}) {
        my $worker = delete $self->{worker}->{$module};
        $self->{file}->{$worker->{filename}}--;

        if ($self->{file}->{$worker->{filename}} < 1) {
            delete $INC{$worker->{filename}};
        }
    }

    $self;
}

=head2 function1

=cut

sub reload_processor {
    my ($self, $module) = @_;

    unless (defined $module) {
        Carp::croak 'missing parameters';
    }

    if (exists $self->{processor}->{$module}) {
        my $processor = $self->{processor}->{$module};
        delete $INC{$processor->{filename}};
        $self->{reload_processor}->{$module} = 1;

        my $fullpath =
            $self->{current_filename} = $processor->{filename};
        my $cwd = getcwd;
        unless (my $ret = do $fullpath) {
            $self->{logger}->warn("couldn't parse $fullpath: $@") if $@;
            $self->{logger}->warn("couldn't do $fullpath: $!")    unless defined $ret;
            $self->{logger}->warn("couldn't run $fullpath")       unless $ret;
        }
        else {
            $self->{logger}->info('reloaded module file ',$fullpath);
        }
        chdir($cwd);

        unless (exists $self->{processor}->{$module}) {
            $self->{processor}->{$module} = $processor;
        }

        if ($self->{file}->{$processor->{filename}} < 1) {
            delete $INC{$processor->{filename}};
        }

        delete $self->{reload_processor}->{$module};
    }

    $self;
}

=head2 function1

=cut

sub reload_worker {
    my ($self, $module) = @_;

    unless (defined $module) {
        Carp::croak 'missing parameters';
    }

    if (exists $self->{worker}->{$module}) {
        my $worker = $self->{worker}->{$module};
        delete $INC{$worker->{filename}};
        $self->{reload_worker}->{$module} = 1;

        my $fullpath =
            $self->{current_filename} = $worker->{filename};
        my $cwd = getcwd;
        unless (my $ret = do $fullpath) {
            $self->{logger}->warn("couldn't parse $fullpath: $@") if $@;
            $self->{logger}->warn("couldn't do $fullpath: $!")    unless defined $ret;
            $self->{logger}->warn("couldn't run $fullpath")       unless $ret;
        }
        else {
            $self->{logger}->info('reloaded module file ',$fullpath);
        }
        chdir($cwd);

        unless (exists $self->{worker}->{$module}) {
            $self->{worker}->{$module} = $worker;
        }

        if ($self->{file}->{$worker->{filename}} < 1) {
            delete $INC{$worker->{filename}};
        }

        delete $self->{reload_worker}->{$module};
    }

    $self;
}

=head2 function1

=cut

sub register_processor {
    my ($self, $module, $func) = @_;

    unless (defined $module and ref($func) eq 'HASH') {
        Carp::croak 'missing parameters';
    }

    foreach (values %$func) {
        unless (ref($_) eq 'CODE') {
            Carp::croak 'found something else then code in function hash';
        }
    }

    if (exists $self->{reload_processor}->{$module}) {
        delete $self->{processor}->{$module};
        $self->{file}->{$self->{current_filename}}--;
    }

    unless (exists $self->{processor}->{$module}) {
        $self->{logger}->info('registered processor module ', $module, ' functions: ', join(', ', keys %$func));

        $self->{processor}->{$module} = {
            module => $module,
            func => $func,
            filename => $self->{current_filename}
        };

        $self->{file}->{$self->{current_filename}}++;
    }

    $self;
}

=head2 function1

=cut

sub register_worker {
    my ($self, $module, $func) = @_;

    unless (defined $module and ref($func) eq 'HASH') {
        Carp::croak 'missing parameters';
    }

    foreach (values %$func) {
        unless (ref($_) eq 'CODE') {
            Carp::croak 'found something else then code in function hash';
        }
    }

    if (exists $self->{reload_worker}->{$module}) {
        delete $self->{worker}->{$module};
        $self->{file}->{$self->{current_filename}}--;
    }

    unless (exists $self->{worker}->{$module}) {
        $self->{logger}->info('registered worker module ', $module, ' functions: ', join(', ', keys %$func));

        $self->{worker}->{$module} = {
            module => $module,
            func => $func,
            filename => $self->{current_filename}
        };

        $self->{file}->{$self->{current_filename}}++;
    }

    $self;
}

=head2 function1

=cut

sub call_processor {
    my ($self, $module, $function, @parameters) = @_;

    unless (exists $self->{processor}->{$module} and exists $self->{processor}->{$module}->{func}->{$function}) {
        $self->{logger}->warn('not able to call processor ', $module, ' function ', $function);
        return;
    }

    return $self->{processor}->{$module}->{func}->{$function}->(@parameters);
}

=head2 function1

=cut

sub call_worker {
    my ($self, $module, $function, @parameters) = @_;

    unless (exists $self->{worker}->{$module} and exists $self->{worker}->{$module}->{func}->{$function}) {
        $self->{logger}->warn('not able to call worker ', $module, ' function ', $function);
        return;
    }

    return $self->{worker}->{$module}->{func}->{$function}->(@parameters);
}

=head2 function1

=cut

sub have_processor {
    defined $_[1] and exists $_[0]->{processor}->{$_[1]};
}

=head2 function1

=cut

sub have_worker {
    defined $_[1] and exists $_[0]->{worker}->{$_[1]};
}

=head2 function1

=cut

sub have_processor_function {
    defined $_[1] and exists $_[0]->{processor}->{$_[1]} and
        defined $_[2] and exists $_[0]->{processor}->{$_[1]}->{func}->{$_[2]};
}

=head2 function1

=cut

sub have_worker_function {
    defined $_[1] and exists $_[0]->{worker}->{$_[1]} and
        defined $_[2] and exists $_[0]->{worker}->{$_[1]}->{func}->{$_[2]};
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
