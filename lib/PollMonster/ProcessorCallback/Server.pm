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

package PollMonster::ProcessorCallback::Server;

use common::sense;
use Carp;

use PollMonster qw(:name);
use PollMonster::RPC::Server;

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
        logger => Log::Log4perl->get_logger()
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
    if (exists $args{on_error} and ref($args{on_error} eq 'CODE')) {
        $self->{on_error} = $args{on_error};
    }
    delete $args{on_error};
    delete $args{on_eof};
    delete $args{on_noauth};

    if (exists $args{on_result} and ref($args{on_result}) eq 'CODE') {
        $self->{on_result} = $args{on_result};
    }
    else {
        croak 'Missing on_result, pointless creating server without it.';
    }

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(result error)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{result} = sub { $self->call_result(@_) };
    $args{service}->{error} = sub { $self->call_error(@_) };

    $args{name} = PROCESSOR_CALLBACK_NAME;
    $self->{rpc} = PollMonster::RPC::Server->new(%args);

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    my ($self) = @_;

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('destroy ', __PACKAGE__, ' ', $self);
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

sub call_result {
    my ($self, $rpc, $cli, $result, $done) = @_;

    $self->{on_result}->($self, $result, $done);

    $rpc->call($cli, 'result', 1);
    return;
}

=head2 function2

=cut

sub call_error {
    my ($self, $rpc, $cli, $errno, $errstr) = @_;

    if (exists $self->{on_error}) {
        $self->{on_error}->($self, $errno, $errstr);
    }
    else {
        $self->{on_result}->($self, undef, 1);
    }

    return;
}

=head2 function2

=cut

sub uri {
    defined $_[0]->{rpc} ? $_[0]->{rpc}->uri : undef;
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
