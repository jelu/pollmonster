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

package PollMonster::Worker::Server;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Server;
use PollMonster::ProcessManager::Client;
use PollMonster::ModuleFactory;

use Log::Log4perl ();
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
        logger => Log::Log4perl->get_logger(),
        registered => 0,
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

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(ready work)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{ready} = sub { $self->call_ready(@_) };
    $args{service}->{work} = sub { $self->call_work(@_) };
    $args{service}->{load} = sub { $self->call_load(@_) };
    $args{service}->{reload} = sub { $self->call_reload(@_) };
    $args{service}->{unload} = sub { $self->call_unload(@_) };

    $args{name} = WORKER_NAME;
    $self->{rpc} = PollMonster::RPC::Server->new(%args);

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
        $self->{rpc}->close();
        $self->{rpc} = undef;
    }

    $self;
}

=head2 function2

=cut

sub call_ready {
    my ($self, $rpc, $cli) = @_;

    $rpc->call($cli, 'ready');

    return;
}

=head2 function2

=cut

sub call_work {
    my ($self, $rpc, $cli, $uuid, $module, $payload, $private) = @_;

    unless (PollMonster::ModuleFactory->instance->have_worker_function($module, 'run')) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'work',
                       errno => 1,
                       errstr => 'module not found or broken'
                   ));
        return;
    }

    $payload = PollMonster::ModuleFactory->instance->call_worker($module, 'run', $payload);
        
    if (ref($payload) eq 'ARRAY') {
        if (defined $private) {
            $rpc->call($cli, 'result_ready', $uuid, $module, $payload, $private);
        }
        else {
            $rpc->call($cli, 'result_ready', $uuid, $module, $payload);
        }
    }
    else {
        $rpc->call($cli, 'ready');
    }

    return;
}

=head2 function2

=cut

sub call_load {
    my ($self, $rpc, $cli, $entry) = @_;

    PollMonster::ModuleFactory->instance->load($entry);

    $rpc->call($cli, 'load', 1);
    return;
}

=head2 function2

=cut

sub call_reload {
    my ($self, $rpc, $cli, $type, $module) = @_;

    if ($type eq 'processor') {
        unless (PollMonster::ModuleFactory->instance->have_processor($module)) {
            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'reload',
                           errno => 1,
                           errstr => 'module does not exists'
                       ));
            return;
        }

        PollMonster::ModuleFactory->instance->reload_processor($module);
    }
    elsif ($type eq 'worker') {
        unless (PollMonster::ModuleFactory->instance->have_worker($module)) {
            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'reload',
                           errno => 1,
                           errstr => 'module does not exists'
                       ));
            return;
        }

        PollMonster::ModuleFactory->instance->reload_worker($module);
    }
    else {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'reload',
                       errno => 1,
                       errstr => 'unknown type'
                   ));
    }

    $rpc->call($cli, 'reload', 1);
    return;
}

=head2 function2

=cut

sub call_unload {
    my ($self, $rpc, $cli, $type, $module) = @_;

    if ($type eq 'processor') {
        PollMonster::ModuleFactory->instance->unload_processor($module);
    }
    elsif ($type eq 'worker') {
        PollMonster::ModuleFactory->instance->unload_worker($module);
    }
    else {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'unload',
                       errno => 1,
                       errstr => 'unknown type'
                   ));
    }

    $rpc->call($cli, 'unload', 1);
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
