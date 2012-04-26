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

package PollMonster::CLI;

use common::sense;
use Carp;

use PollMonster qw(:name);
use PollMonster::API;
use PollMonster::ProcessManager::Client;
use PollMonster::OrderManager::Client;
use PollMonster::ModuleFactory;

use Coro ();
use Coro::Signal ();
use Coro::Timer ();
use AnyEvent::Handle ();

use Log::Log4perl ();
use POSIX qw(getpid strftime);

#use Curses;
use IO::Handle ();

use Data::Dumper ();

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

our $VERSION = $PollMonster::VERSION;

our $COMMANDS = {
    exit => \&cmd_close,
    quit => \&cmd_close,
    help => \&cmd_help,
    list => \&cmd_list,
    add_order => \&cmd_add_order,
    add_order_callback => \&cmd_add_order_callback,
    list_orders => \&cmd_list_orders,
    remove_order => \&cmd_remove_order,
    stop_order => \&cmd_stop_order,
    timeslots => \&cmd_timeslots,
    exec => \&cmd_exec,
    load => \&cmd_load,
    reload_processor => \&cmd_reload_processor,
    reload_worker => \&cmd_reload_worker
};

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
    my $class = shift;
    my $self = bless {
    }, $class;

    #initscr;
    #cbreak;
    #noecho;

    autoflush STDOUT 1;

    $self->{handle} = AnyEvent::Handle->new(
        fh => \*STDIN,
        on_read => sub {
            my ($handle) = @_;
            my $input = $handle->{rbuf};
            $handle->{rbuf} = '';

            $input =~ s/[\r\n]+$//o;
            my ($cmd, @parameters) = split(/\s+/o, $input);

            if (defined $cmd) {
                unless (defined $COMMANDS->{$cmd}) {
                    print "command not found\n";
                } else {
                    $COMMANDS->{$cmd}->($self, @parameters);
                    return;
                }
            }
            $self->prompt;
        }
        );

    $self->prompt;

    $self;
}

=head2 function2

=cut

sub cmd_close {
    my ($self) = @_;

    if (defined $self->{handle}) {
        $self->{handle}->push_shutdown;
        delete $self->{handle};
    }

    kill 2, getpid;

    $self;
}

=head2 function2

=cut

sub prompt {
    print 'pollmonster> ';
    STDOUT->flush;
}

=head2 function2

=cut

sub cmd_help {
    $_[0]->prompt;
}

=head2 function2

=cut

sub cmd_list {
    my ($self, $type) = @_;
    my $list = PollMonster::API->instance->list({ type => $type });

    if (ref($list) eq 'ARRAY') {
        printf "-#   %6s %-20s %-16s %-19s %s\n", 'PID', 'SERVICE', 'HOSTNAME', 'LAST HEARTBEAT', 'URI';
        
        my $count = 1;
        foreach (@$list) {
            if (!($count % 20)) {
                STDOUT->flush;
                printf "\n-#   %6s %-20s %-16s %19s %s\n", 'PID', 'SERVICE', 'HOSTNAME', 'LAST HEARTBEAT', 'URI';
            }

            printf "#%-3d %6d %-20s %-16s %19s %s\n", $count++, $_->{pid}, $_->{service}, $_->{hostname}, strftime('%Y-%m-%d %H:%M:%S', localtime($_->{heartbeat})), $_->{uri};
        }
    }
    else {
        print "Error getting list\n";
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_add_order {
    my ($self, $name, $json) = @_;
    my ($order_data, @list, $uuid);
    my $sig = Coro::Signal->new;

    eval {
        $order_data = JSON::XS->new->ascii->decode($json);
    };
    if ($@) {
        print "json error: $@\n";
        $self->prompt;
        return;
    }

    my $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            shift->list(ORDER_MANAGER_NAME);
        },
        service => {
            list => sub {
                shift;
                @list = @_;
                $sig->send;
            }
        }
        );
    $sig->wait;

    foreach my $om (@list) {
        my $om = PollMonster::OrderManager::Client->new(
            uri => $om->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->add($name, $order_data);
            },
            service => {
                add => sub {
                    my (undef, $id) = @_;
                    if (defined $id) {
                        $uuid = $id;
                        @list = ();
                    }
                    $sig->send;
                }
            }
            );
        $sig->wait;

        last if (defined $uuid);
    }

    if ($uuid) {
        print "order $uuid added\n";
    }
    else {
        print "add order failed\n";
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_add_order_callback {
    my ($self, $name, $json) = @_;
    my ($uuid, $pc_rpc, $order_data);

    eval {
        $order_data = JSON::XS->new->ascii->decode($json);
    };
    if ($@) {
        print "json error: $@\n";
        $self->prompt;
        return;
    }

    my ($errno, $errstr, $result) = PollMonster::API->instance->add_order_callback({ module => $name, parameter => $order_data });
    if (defined $errno) {
        print "error $errno: $errstr\n";
    }
    else {
        print Data::Dumper::Dumper($result);
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_list_orders {
    my ($self) = @_;
    my (@list, @orders);
    my $sig = Coro::Signal->new;

    my $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            shift->list(ORDER_MANAGER_NAME);
        },
        service => {
            list => sub {
                shift;
                @list = @_;
                $sig->send;
            }
        }
        );
    $sig->wait;

    foreach my $om (@list) {
        my $om = PollMonster::OrderManager::Client->new(
            uri => $om->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->list;
            },
            service => {
                list => sub {
                    (undef, @orders) = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        last if (@orders);
    }

    printf "-#    %-26s %-24s %s\n", 'UUID', 'MODULE', 'PROCESSOR';
    printf "-#    %s\n", 'PARAMETERS';

    my $count = 1;
    foreach (@orders) {
        if (!($count % 20)) {
            STDOUT->flush;
            printf "\n-#    %-26s %-24s %s\n", 'UUID', 'MODULE', 'PROCESSOR';
            printf "-#    %s\n", 'PARAMETERS';
        }

        printf "#%-4d %-26s %-24s %s\n", $count++, $_->{uuid}, $_->{module}, $_->{processor};

        my @parameters;
        foreach my $key (sort { $a cmp $b } keys %{$_->{parameter}}) {
            push(@parameters, $key.' => '.$_->{parameter}->{$key});
        }

        if (@parameters) {
            printf "      %s\n", join(', ', @parameters);
        }
        else {
            printf "      %s\n", '(none)';
        }
    }

    print scalar @orders, " number of orders\n";
    STDOUT->flush;

    $self->prompt;
}

=head2 function2

=cut

sub cmd_remove_order {
    my ($self, $uuid) = @_;
    my (@list, $removed);
    my $sig = Coro::Signal->new;

    my $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            shift->list(ORDER_MANAGER_NAME);
        },
        service => {
            list => sub {
                shift;
                @list = @_;
                $sig->send;
            }
        }
        );
    $sig->wait;

    $removed = 0;
    foreach my $om (@list) {
        my $om = PollMonster::OrderManager::Client->new(
            uri => $om->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->remove($uuid);
            },
            service => {
                remove => sub {
                    my (undef, $res) = @_;

                    if (ref($res)) {
                        print $res->to_string,"\n";
                    }
                    elsif($res == 1) {
                        $removed = 1;
                    }
                    $sig->send;
                }
            }
            );
        $sig->wait;

        last if ($removed);
    }

    if ($removed) {
        print "order $uuid removed\n";
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_stop_order {
    my ($self, $uuid) = @_;
    my (@list, $stopped);
    my $sig = Coro::Signal->new;

    my $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            shift->list(ORDER_MANAGER_NAME);
        },
        service => {
            list => sub {
                shift;
                @list = @_;
                $sig->send;
            }
        }
        );
    $sig->wait;

    $stopped = 0;
    foreach my $om (@list) {
        my $om = PollMonster::OrderManager::Client->new(
            uri => $om->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->stop($uuid);
            },
            service => {
                stop => sub {
                    my (undef, $res) = @_;

                    if (ref($res)) {
                        print $res->to_string,"\n";
                    }
                    elsif($res == 1) {
                        $stopped = 1;
                    }
                    $sig->send;
                }
            }
            );
        $sig->wait;

        last if ($stopped);
    }

    if ($stopped) {
        print "order $uuid stopped\n";
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_timeslots {
    my ($self, $uuid) = @_;
    my (@list, $timeslots);
    my $sig = Coro::Signal->new;

    my $pm = PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub { $sig->send },
        on_connect => sub {
            shift->list(TIMESLOT_MANAGER_NAME);
        },
        service => {
            list => sub {
                shift;
                @list = @_;
                $sig->send;
            }
        }
        );
    $sig->wait;

    foreach my $tsm (@list) {
        my $tsm = PollMonster::TimeSlotManager::Client->new(
            uri => $tsm->{uri},
            on_error => sub { $sig->send },
            on_connect => sub {
                shift->timeslots;
            },
            service => {
                timeslots => sub {
                    (undef, $timeslots) = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if (defined $timeslots) {
            print $tsm->timeslots_to_string($timeslots),"\n";
            last;
        }
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_exec {
    my ($self, $file) = @_;

    unless (open(FILE, $file)) {
        print "Unable to open file\n";
    }
    else {
        while(<FILE>) {
            s/[\r\n]+$//o;
            my ($cmd, @parameters) = split(/\s+/o);

            if (defined $cmd) {
                if (defined $COMMANDS->{$cmd}) {
                    print join(' ', $cmd, @parameters),"\n";
                    $COMMANDS->{$cmd}->($self, @parameters);
                }
            }
        }
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_load {
    my ($self, $entry) = @_;
    my (@om_list, @op_list, @w_list);

    if (defined $entry) {
        my $sig = Coro::Signal->new;
        my $sem = Coro::Semaphore->new;
        my $err;

        my $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(ORDER_MANAGER_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @om_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of OrderManagers\n";
            $self->prompt;
            return;
        }

        $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(ORDER_PROCESSOR_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @op_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of OrderProcessors\n";
            $self->prompt;
            return;
        }

        my $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(WORKER_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @w_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of Workers\n";
            $self->prompt;
            return;
        }

        print "Sending load...\n";

        my @clients;

        foreach my $om (@om_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::OrderManager::Client->new(
                uri => $om->{uri},
                on_error => sub { $sem->up },
                on_connect => sub {
                    shift->load($entry);
                },
                service => {
                    load => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'load failed at ', $om->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        foreach my $op (@op_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::OrderProcessor::Client->new(
                uri => $op->{uri},
                on_error => sub { $sem->up },
                on_connect => sub {
                    shift->load($entry);
                },
                service => {
                    load => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'load failed at ', $op->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        foreach my $w (@w_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::Worker::Client->new(
                uri => $w->{uri},
                on_error => sub { $sem->up; },
                on_connect => sub {
                    shift->load($entry);
                },
                service => {
                    load => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'load failed at ', $w->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        print "Waiting for completion...\n";
        $sem->wait;
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_reload_processor {
    my ($self, $module) = @_;
    my (@om_list, @op_list);

    if (defined $module) {
        my $sig = Coro::Signal->new;
        my $sem = Coro::Semaphore->new;
        my $err;

        my $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(ORDER_MANAGER_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @om_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of OrderManagers\n";
            $self->prompt;
            return;
        }

        $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(ORDER_PROCESSOR_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @op_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of OrderProcessors\n";
            $self->prompt;
            return;
        }

        print "Sending reload...\n";

        my @clients;

        foreach my $om (@om_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::OrderManager::Client->new(
                uri => $om->{uri},
                on_error => sub { $sem->up },
                on_connect => sub {
                    shift->reload('processor', $module);
                },
                service => {
                    reload => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'reload failed at ', $om->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        foreach my $op (@op_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::OrderProcessor::Client->new(
                uri => $op->{uri},
                on_error => sub { $sem->up },
                on_connect => sub {
                    shift->reload('processor', $module);
                },
                service => {
                    reload => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'reload failed at ', $op->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        print "Waiting for completion...\n";
        $sem->wait;
    }

    $self->prompt;
}

=head2 function2

=cut

sub cmd_reload_worker {
    my ($self, $module) = @_;
    my (@om_list, @op_list, @w_list);

    if (defined $module) {
        my $sig = Coro::Signal->new;
        my $sem = Coro::Semaphore->new;
        my $err;

        my $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(ORDER_MANAGER_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @om_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of OrderManagers\n";
            $self->prompt;
            return;
        }

        $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(ORDER_PROCESSOR_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @op_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of OrderProcessors\n";
            $self->prompt;
            return;
        }

        my $pm = PollMonster::ProcessManager::Client->new(
            uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
            on_error => sub { $err = 1; $sig->send },
            on_connect => sub {
                shift->list(WORKER_NAME);
            },
            service => {
                list => sub {
                    shift;
                    @w_list = @_;
                    $sig->send;
                }
            }
            );
        $sig->wait;

        if ($err) {
            print "Error getting list of Workers\n";
            $self->prompt;
            return;
        }

        print "Sending reload...\n";

        my @clients;

        foreach my $om (@om_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::OrderManager::Client->new(
                uri => $om->{uri},
                on_error => sub { $sem->up },
                on_connect => sub {
                    shift->reload('worker', $module);
                },
                service => {
                    reload => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'reload failed at ', $om->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        foreach my $op (@op_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::OrderProcessor::Client->new(
                uri => $op->{uri},
                on_error => sub { $sem->up },
                on_connect => sub {
                    shift->reload('worker', $module);
                },
                service => {
                    reload => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'reload failed at ', $op->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        foreach my $w (@w_list) {
            $sem->adjust(-1);
            push @clients, PollMonster::Worker::Client->new(
                uri => $w->{uri},
                on_error => sub { $sem->up; },
                on_connect => sub {
                    shift->reload('worker', $module);
                },
                service => {
                    reload => sub {
                        my (undef, $ok) = @_;

                        unless ($ok == 1) {
                            print 'reload failed at ', $w->{uri}, (ref($ok) ? ': '.$ok->errstr : ''), "\n";
                        }
                        $sem->up;
                    }
                }
                );
        }

        print "Waiting for completion...\n";
        $sem->wait;
    }

    $self->prompt;
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
