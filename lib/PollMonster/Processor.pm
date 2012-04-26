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

package PollMonster::Processor;

use common::sense;
use Carp;

use PollMonster qw(:name);
use PollMonster::ModuleFactory;
use PollMonster::TimeSlotManager::Client;
use PollMonster::ProcessorCallback::Client;

use Coro ();
use Coro::Timer ();
use Coro::Signal ();

use Log::Log4perl ();
use Scalar::Util qw(blessed);

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
    my $self = {
        logger => Log::Log4perl->get_logger(),
        option => {},
        parameter => {},
        status => 'none',
        errstr => '',
        end => 0,
        runs => 0,
        results => [],
        result_signal => Coro::Signal->new,
        state => {},
        @_
    };
    bless $self, $class;

    unless (defined $self->{uuid}) {
        croak 'uuid undefined';
    }
    unless (defined $self->{module}) {
        croak 'module name undefined';
    }
    unless (defined $self->{on_work}) {
        croak 'on_work callback must be defined';
    }
    unless (defined $self->{on_end}) {
        croak 'on_end callback must be defined';
    }

    unless (PollMonster::ModuleFactory->instance->have_processor_function($self->{module}, 'run')) {
        croak 'module does not have run subroutine';
    }

    $self->{coro} = new Coro sub {
        while () {
            $self->{end} = 0;
            $self->{status} = 'running';

            eval {
                PollMonster::ModuleFactory->instance->call_processor(
                    $self->{module}, 'run', $self, $self->{uuid}, $self->{parameter});
            };
            if ($@) {
                $self->{errstr} = $@;
                $self->{logger}->warn('module ', $self->{module}, ' died on: ', $@);
                $self->{status} = 'died';
            }
            else {
                $self->{end} = 1;
            }

            if ($self->{status} eq 'reloading') {
                $self->{logger}->info('reloading module ', $self->{module});
                next;
            }
            elsif ($self->{status} eq 'restarting') {
                $self->{logger}->info('restarting module ', $self->{module});
                next;
            }

            $self->{logger}->info('module ', $self->{module}, ' ending with status ', $self->{status});
            last;
        }

        if ($self->{end}) {
            $self->{status} = 'ended';
            $self->{on_end}->($self);
        }
    };
    $self->{coro}->prio(Coro::PRIO_LOW);
    $self->{coro}->desc($self->{module}, ' [uuid: ', $self->{uuid}, ']');

    PollMonster::OBJ_DEBUG and $self->{logger}->debug('new ', __PACKAGE__, ' ', $self);

    $self;
}

sub DESTROY {
    PollMonster::OBJ_DEBUG and $_[0]->{logger}->debug('destroy ', __PACKAGE__, ' ', $_[0]);
}

=head2 function1

=cut

sub start {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{coro}->ready;
        $self->{logger}->info('start order ', $self->{uuid}, ' module ', $self->{module});
    }

    $self;
}

=head2 function1

=cut

sub stop {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{status} = 'stopping';
        $self->{end} = 1;
    }

    $self;
}

=head2 function1

=cut

sub restart {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{status} = 'restarting';
        $self->{end} = 1;
    }

    $self;
}

=head2 function1

=cut

sub terminate {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{coro}->cancel;
        $self->{coro} = undef;
        $self->{status} = 'killed';
        $self->{end} = 1;
    }

    $self;
}

=head2 function1

=cut

sub suspend {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{coro}->suspend;
        $self->{status} = 'suspended';
    }

    $self;
}

=head2 function1

=cut

sub resume {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{coro}->ready;
        $self->{status} = 'running';
    }

    $self;
}

=head2 function1

=cut

sub remove {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{status} = 'removing';
        $self->{end} = 1;
    }

    $self;
}
    
=head2 function1

=cut

sub reload {
    my ($self) = @_;

    if (defined $self->{coro}) {
        $self->{status} = 'reloading';
        $self->{end} = 1;
        $self->{logger}->info('module ', $self->{module}, ' tagged for reload');
    }

    $self;
}

=head2 function1

=cut

sub end {
    $_[0]->{end};
}

=head2 function1

=cut

sub work {
    my ($self, $payload, $private) = @_;

    unless (blessed($payload) and $payload->isa('PollMonster::Payload::Work') and $payload->is_valid) {
        $self->{logger}->warn('payload is invalid (', defined $payload ? $payload : 'undef', ')');
        return;
    }

    unless (PollMonster::ModuleFactory->instance->call_worker($payload->module, 'verify', $payload)) {
        $self->{logger}->warn('payload did not verify');
        return;
    }

    $payload->set_uuid($self->{uuid});

    if (defined $private) {
        $payload->set_private($private);
    }

    my $ret = $self->{on_work}->($self, $payload);
    Coro::cede;
    return $ret;
}

=head2 function1

=cut

sub work_wait {
    my ($self, $module, $payload, $private) = @_;

    # todo
}

=head2 function1

=cut

sub push_result {
    my ($self, $module, $payload, $private) = @_;

    if (defined (my $result = PollMonster::ModuleFactory->instance->call_worker($module, 'to_result', $payload))) {
        if (defined $private) {
            $result->set_private($private);
        }

        push(@{$self->{results}}, $result);
        $self->{result_signal}->broadcast;
    }

    return;
}

=head2 function1

=cut

sub result {
    Coro::cede;
    return shift(@{$_[0]->{results}});
}

=head2 function1

=cut

sub result_wait {
    my ($self) = @_;

    unless (@{$self->{results}}) {
        my $old_status = $self->{status};
        $self->{status} = 'waiting for work';
        Coro::cede;
        unless (@{$self->{results}}) {
            $self->{result_signal}->wait;
        }
        $self->{status} = $old_status;
    }

    return shift(@{$self->{results}});
}

=head2 function1

=cut

sub result_wait_timeout {
    my ($self, $timeout) = @_;
    my ($signal, $timer);

    unless (@{$self->{results}}) {
        my $old_status = $self->{status};
        $self->{status} = 'waiting for work';
        Coro::cede;
        unless (@{$self->{results}}) {
            $signal = Coro::Signal->new;
            $timer = AnyEvent->timer(
                after => defined $timeout ? $timeout : 30,
                cb => sub { $signal->send });

            Coro::async_pool {
                $self->{result_signal}->wait;
                $signal->send;
            };

            $signal->wait;
        }
        $self->{status} = $old_status;
    }

    return shift(@{$self->{results}});
}

=head2 function1

=cut

sub callback_result {
    my ($self, $result, $done) = @_;

    if (defined $self->{option}->{callback}) {
        my $pc_rpc; $pc_rpc = PollMonster::ProcessorCallback::Client->new(
            uri => $self->{option}->{callback},
            on_error => sub { $pc_rpc = undef },
            on_connect => sub {
                shift->result($result, $done);
            },
            service => {
                result => sub {
                    $pc_rpc = undef;
                }
            }
            );
    }

    $self;
}

=head2 function1

=cut

sub ran {
    $_[0]->{runs}++;
}

=head2 function1

=cut

sub runs {
    $_[0]->{runs};
}

=head2 function1

=cut

sub sleep {
    my ($self, $timeout) = @_;

    if (defined $timeout and $timeout > 0) {
        my $old_status = $self->{status};
        $self->{status} = 'sleeping';
        Coro::Timer::sleep($timeout);
        $self->{status} = $old_status;
    }

    $self;
}

=head2 function1

=cut

sub status {
}

=head2 function1

=cut

sub state {
    $_[0]->{state};
}

=head2 function1

=cut

sub timeslot {
    my ($self, $slot_size, $work_avg, $after) = @_;
    my (@list, @o, $timeout, $signal, $ok);

    unless (defined $slot_size and defined $work_avg) {
        croak 'missing parameters';
    }

    $timeout = 0;
    $signal = Coro::Signal->new;
    push @o, AnyEvent->timer(
        after => 5,
        cb => sub { $timeout = 1; $signal->send; });
    push @o, PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub {
            $signal->send;
        },
        on_connect => sub {
            $_[0]->list(TIMESLOT_MANAGER_NAME);
        },
        service => {
            list => sub {
                (undef, @list) = @_;
                $signal->send;
            }
        });
    $signal->wait;
    @o = ();

    if ($timeout) {
        $self->{logger}->warn('timeout retriving workers');
        return;
    }

    foreach my $tsm (@list) {
        my $start = 0;
        $timeout = 0;
        $signal = Coro::Signal->new;
        push @o, AnyEvent->timer(
            after => 5,
            cb => sub { $timeout = 1; $signal->send; });
        push @o, PollMonster::TimeSlotManager::Client->new(
            uri => $tsm->{uri},
            on_error => sub {
                $signal->send;
            },
            on_connect => sub {
                if (defined $after) {
                    $_[0]->allocate($self->{uuid}, $slot_size, $work_avg, $after);
                }
                else {
                    $_[0]->allocate($self->{uuid}, $slot_size, $work_avg);
                }
            },
            service => {
                allocate => sub {
                    (undef, $start) = @_;
                    $signal->send;
                }
            });
        $signal->wait;
        @o = ();

        if ($timeout) {
            $self->{logger}->warn('timeout getting timeslot from timeslot-manager ', $tsm->{uri});
            next;
        }

        unless (defined $start) {
            $self->{logger}->warn('unable to get timeslot from timeslot-manager ', $tsm->{uri});
            next;
        }

        return $start;
    }

    return;
}

=head2 function1

=cut

sub uuid {
    $_[0]->{uuid};
}

=head2 function1

=cut

sub errstr {
    $_[0]->{errstr};
}

=head2 function1

=cut

sub debug {
    my $self = shift;

    $self->{logger}->debug($self->{module}, '@', $self->{uuid}, ': ', @_);
}

=head2 function1

=cut

sub info {
    my $self = shift;

    $self->{logger}->info($self->{module}, '@', $self->{uuid}, ': ', @_);
}

=head2 function1

=cut

sub warn {
    my $self = shift;

    $self->{logger}->warn($self->{module}, '@', $self->{uuid}, ': ', @_);
}

=head2 function1

=cut

sub error {
    my $self = shift;

    $self->{logger}->error($self->{module}, '@', $self->{uuid}, ': ', @_);
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
