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

package PollMonster;

use common::sense;

use base qw(Exporter);

our @EXPORT_OK = qw(
RPC_DEBUG OBJ_DEBUG DEBUG INFO
NO_DELAY AUTOCORK READ_SIZE READ_BUF CHUNK_BUF SRV_LISTEN CLI_TIMEOUT
EUNKNOWN ENOMETHOD ENETCTIME
GLOBAL_NAME
PROCESS_MANAGER_NAME
ORDER_MANAGER_NAME
ORDER_PROCESSOR_NAME
WORK_DISPATCHER_NAME
WORKER_NAME
TIMESLOT_MANAGER_NAME
MODULE_FACTORY_NAME
LOGGER_NAME
APIBRIDGE_JSONP_NAME
APIBRIDGE_JSON_NAME
PROCESSOR_CALLBACK_NAME);

our %EXPORT_TAGS = (
    logger => [qw(RPC_DEBUG OBJ_DEBUG DEBUG INFO)],
    socket => [qw(NO_DELAY AUTOCORK READ_SIZE READ_BUF CHUNK_BUF SRV_LISTEN CLI_TIMEOUT)],
    errno => [qw(EUNKNOWN ENOMETHOD ENETCTIME)],
    name => [qw(GLOBAL_NAME
PROCESS_MANAGER_NAME
ORDER_MANAGER_NAME
ORDER_PROCESSOR_NAME
WORK_DISPATCHER_NAME
WORKER_NAME
TIMESLOT_MANAGER_NAME
MODULE_FACTORY_NAME
LOGGER_NAME
APIBRIDGE_JSONP_NAME
APIBRIDGE_JSON_NAME
PROCESSOR_CALLBACK_NAME)]
    );

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.03';

sub RPC_DEBUG (){ 1 }
sub OBJ_DEBUG (){ 1 }
sub DEBUG (){ 1 }
sub INFO (){ 1 }

sub NO_DELAY    (){ 0 }
sub AUTOCORK    (){ 1 }
sub READ_SIZE   (){ 1024 * 32 }
sub READ_BUF    (){ 1024 * 1024 * 4 }
sub SNDBUF      (){ 1024 * 256 }
sub RCVBUF      (){ 1024 * 256 }
sub CHUNK_BUF   (){ 128 }
sub SRV_LISTEN  (){ 10 }
sub CLI_TIMEOUT (){ 10 }

sub EUNKNOWN  (){ -1 }
sub ENOMETHOD (){ -2 }
sub ENETCTIME (){ -3 }

our %ERRSTR = (
    PollMonster::EUNKNOWN => 'Unknown error',
    PollMonster::ENOMETHOD => 'No such method',
    PollMonster::ENETCTIME => 'Network: identical ctime exists'
    );

sub GLOBAL_NAME             (){ 'global' }
sub PROCESS_MANAGER_NAME    (){ 'process-manager' }
sub ORDER_MANAGER_NAME      (){ 'order-manager' }
sub ORDER_PROCESSOR_NAME    (){ 'order-processor' }
sub WORK_DISPATCHER_NAME    (){ 'work-dispatcher' }
sub WORKER_NAME             (){ 'worker' }
sub TIMESLOT_MANAGER_NAME   (){ 'timeslot-manager' }
sub MODULE_FACTORY_NAME     (){ 'module-factory' }
sub LOGGER_NAME             (){ 'logger' }
sub APIBRIDGE_JSONP_NAME    (){ 'apibridge-jsonp' }
sub APIBRIDGE_JSON_NAME     (){ 'apibridge-json' }
sub PROCESSOR_CALLBACK_NAME (){ 'processor-callback' }

sub PROCESS_LIST_INTERVAL   (){ 15 }
sub HEARTBEAT_INTERVAL      (){ 45 }
sub HEARTBEAT_TIMEOUT       (){ 300 }

our $CONFIG ||= {};

sub CFG {
    my (undef, $section, $parameter, $default) = @_;

    unless (defined $section) {
        return;
    }

    unless (exists $CONFIG->{$section}) {
        return $default;
    }

    unless (defined $parameter) {
        return $CONFIG->{$section};
    }

    unless (exists $CONFIG->{$section}->{$parameter}) {
        return $default;
    }

    $CONFIG->{$section}->{$parameter};
}

sub SET_CFG {
    my (undef, $section, $parameter, $value) = @_;

    if (defined $section and defined $parameter and defined $value) {
        $CONFIG->{$section}->{$parameter} = $value;
    }
}

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use PollMonster;

    my $foo = PollMonster->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

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
