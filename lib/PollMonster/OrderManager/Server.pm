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

package PollMonster::OrderManager::Server;

use common::sense;
use Carp;

use base qw(PollMonster::ProcessManager::Helper);

use PollMonster qw(:name);
use PollMonster::RPC::Network;
use PollMonster::ModuleFactory;
use PollMonster::OrderProcessor::Client;
use PollMonster::OrderManager::Order;
use PollMonster::OrderManager::Processor;

use AnyEvent ();
use Coro ();
use Coro::Signal ();
use Coro::Semaphore ();

use Log::Log4perl ();
use Data::UUID ();
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
        logger => Log::Log4perl->get_logger,
        registered => 0,
        order => {},
        processor => {},
        order_push_semaphore => Coro::Semaphore->new
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
    $args{on_net_join} = sub { $self->on_net_join(@_); };

    if (exists $args{service} and ref($args{service}) eq 'HASH') {
        foreach (qw(add remove list sync-orders sync-add sync-remove sync-update sync-remove-processor)) {
            if (exists $args{service}->{$_}) {
                croak 'Method '.$_.' is not allowed to overload in '.__PACKAGE__;
            }
        }
    }
    else {
        $args{service} = {};
    }

    $args{service}->{add} = sub { $self->call_add(@_) };
    $args{service}->{remove} = sub { $self->call_remove(@_) };
    $args{service}->{ended} = sub { $self->call_ended(@_) };
    $args{service}->{list} = sub { $self->call_list(@_) };
    $args{service}->{stop} = sub { $self->call_stop(@_) };
    $args{service}->{start} = sub { $self->call_start(@_) };
    $args{service}->{restart} = sub { $self->call_restart(@_) };
    $args{service}->{load} = sub { $self->call_load(@_) };
    $args{service}->{reload} = sub { $self->call_reload(@_) };
    $args{service}->{unload} = sub { $self->call_unload(@_) };
    $args{service}->{'sync-orders'} = sub { $self->call_sync_orders(@_) };
    $args{service}->{'sync-add'} = sub { $self->call_sync_add(@_) };
    $args{service}->{'sync-remove'} = sub { $self->call_sync_remove(@_) };
    $args{service}->{'sync-update'} = sub { $self->call_sync_update(@_) };
    $args{service}->{'sync-remove-processor'} = sub { $self->call_sync_remove_processor(@_) };

    $args{name} = ORDER_MANAGER_NAME;
    $self->{rpc} = PollMonster::RPC::Network->new(%args);

    if ($self->{rpc}->is_alive) {
        my ($pt, $ps);
        $ps = Coro::Semaphore->new;
        $pt = AnyEvent->timer(
            after => PollMonster::PROCESS_LIST_INTERVAL,
            interval => PollMonster::PROCESS_LIST_INTERVAL,
            cb => sub {
                Coro::async_pool {
                    if (defined $self) {
                        my $g = $ps->guard;
                        $self->_processors;
                    }
                    else {
                        $ps = undef;
                        $pt = undef;
                    }
                }
            });
    }

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

sub register {
    my ($self, $uri, $callback) = @_;

    unless (ref($callback) eq 'CODE') {
        $callback = undef;
    }

    return $self->SUPER::register(
        $uri,
        sub {
            if ($self->is_registered) {
                my $pm_cli; $pm_cli = PollMonster::ProcessManager::Client->new(
                    uri => $uri,
                    on_error => sub {
                        $pm_cli = undef
                    },
                    on_connect => sub {
                        my ($rpc, $name) = @_;
                        $rpc->list(ORDER_MANAGER_NAME);
                    },
                    service => {
                        list => sub {
                            my ($rpc, @list) = @_;

                            Coro::async_pool {
                                foreach my $om (@list) {
                                    if (defined $self->{rpc}) {
                                        next if ($self->{rpc}->uri eq $om->{uri});
                                        last if ($self->{rpc}->is_joined);
                                        my $signal = Coro::Signal->new;
                                        $self->{rpc}->net_join($om->{uri}, sub { $signal->send });
                                        $signal->wait;
                                    }
                                }
                            };

                            $pm_cli = undef;
                        }
                    }
                    );
            }

            if (defined $callback) {
                $callback->($self);
            }
        }
        );
}

=head2 function2

=cut

sub close {
    my ($self) = @_;

    if (defined $self->{rpc}) {
        $self->{rpc} = undef;
    }

    $self;
}

=head2 function2

=cut

sub call_add {
    my ($self, $rpc, $cli, $module, $parameter, $option) = @_;

    $parameter = PollMonster::ModuleFactory->instance->call_processor($module, 'verify_order', $parameter);
    unless (ref($parameter) eq 'HASH') {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'add',
                       errno => 1,
                       errstr => 'order is wrong'
                   ));
        return;
    }

    my $uuid;

    for (1..10) {
        $uuid = Data::UUID->new->create_b64;
        unless (exists $self->{order}->{$uuid}) {
            last;
        }
    }

    if (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'add',
                       errno => 1,
                       errstr => 'failed to generate uuid'
                   ));
        return;
    }

    $self->{order}->{$uuid} = PollMonster::OrderManager::Order->new(
        uuid => $uuid,
        module => $module,
        parameter => $parameter,
        option => $option
        );

    $self->{rpc}->push_sync('sync-add', $self->{order}->{$uuid});

    if (exists $option->{push} and $self->{rpc}->is_oldest) {
        $self->_order_push;
    }

    $rpc->call($cli, 'add', $uuid);
    return;
}

=head2 function2

=cut

sub call_remove {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'remove',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }
    
    my $order = delete $self->{order}->{$uuid};

    if (defined $order->processor) {
        my (@o, $timeout, $signal, $ok);
        my $processor = $self->{processor}->{$order->processor};

        $timeout = 0;
        $signal = Coro::Signal->new;
        push @o, AnyEvent->timer(
            after => 30,
            cb => sub { $timeout = 1; $signal->send; });
        push @o, PollMonster::OrderProcessor::Client->new(
            uri => $processor->uri,
            on_error => sub {
                $signal->send;
            },
            on_connect => sub {
                $_[0]->remove($order->uuid);
            },
            service => {
                remove => sub {
                    (undef, $ok) = @_;
                    $signal->send;
                }
            });
        $signal->wait;
        @o = ();

        if ($timeout) {
            $self->{logger}->warn('timeout removing order ', $order->uuid,' to order processor ', $processor->uri);

            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'remove',
                           errno => 1,
                           errstr => 'timeout removing order'
                       ));
            return;
        }

        if (ref($ok)) {
            $self->{logger}->warn('error removing order ', $order->uuid,' to order processor ', $processor->uri, ':', $ok->to_string);

            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'remove',
                           errno => 1,
                           errstr => 'error removing order: '.$ok->to_string
                       ));
            return;
        }

        unless ($ok == 1) {
            $self->{logger}->warn('unable to remove order ', $order->uuid,' to order processor ', $processor->uri);

            $rpc->call($cli, PollMonster::RPC::Fault->new(
                           method => 'remove',
                           errno => 1,
                           errstr => 'unable to remove order'
                       ));
            return;
        }

        $processor->del_order($order->uuid);
    }

    $self->{rpc}->push_sync('sync-remove', $uuid);

    $rpc->call($cli, 'remove', 1);
    return;
}

=head2 function2

=cut

sub call_ended {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'ended',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }
    
    my $order = delete $self->{order}->{$uuid};

    if (defined $order->processor) {
        my $processor = $self->{processor}->{$order->processor};
        $processor->del_order($order->uuid);
    }

    $self->{rpc}->push_sync('sync-remove', $uuid);

    $rpc->call($cli, 'ended', 1);
    return;
}

=head2 function2

=cut

sub call_list {
    my ($self, $rpc, $cli) = @_;

    $rpc->call($cli, 'list', values %{$self->{order}});
    return;
}

=head2 function2

=cut

sub call_stop {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }
    
    my $order = $self->{order}->{$uuid};

    unless (defined $order->processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'order not assigned'
                   ));
        return;
    }

    my $processor = $self->{processor}->{$order->processor};

    unless (defined $processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'lost processor of order'
                   ));
        return;
    }

    my (@o, $timeout, $signal, $ok);

    $timeout = 0;
    $signal = Coro::Signal->new;
    push @o, AnyEvent->timer(
        after => 30,
        cb => sub { $timeout = 1; $signal->send; });
    push @o, PollMonster::OrderProcessor::Client->new(
        uri => $processor->uri,
        on_error => sub {
            $signal->send;
        },
        on_connect => sub {
            $_[0]->stop($order->uuid);
        },
        service => {
            stop => sub {
                (undef, $ok) = @_;
                $signal->send;
            }
        });
    $signal->wait;
    @o = ();

    if ($timeout) {
        $self->{logger}->warn('timeout stopping order ', $order->uuid,' at order processor ', $processor->uri);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'timeout stopping order'
                   ));
        return;
    }

    if (ref($ok)) {
        $self->{logger}->warn('error stopping order ', $order->uuid,' at order processor ', $processor->uri, ':', $ok->to_string);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'error stopping order: '.$ok->to_string
                   ));
        return;
    }

    unless ($ok == 1) {
        $self->{logger}->warn('unable to stop order ', $order->uuid,' at order processor ', $processor->uri);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'stop',
                       errno => 1,
                       errstr => 'unable to stop order'
                   ));
        return;
    }

    $rpc->call($cli, 'stop', 1);
    return;
}

=head2 function2

=cut

sub call_start {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }
    
    my $order = $self->{order}->{$uuid};

    unless (defined $order->processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'order not assigned'
                   ));
        return;
    }

    my $processor = $self->{processor}->{$order->processor};

    unless (defined $processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'lost processor of order'
                   ));
        return;
    }

    my (@o, $timeout, $signal, $ok);

    $timeout = 0;
    $signal = Coro::Signal->new;
    push @o, AnyEvent->timer(
        after => 30,
        cb => sub { $timeout = 1; $signal->send; });
    push @o, PollMonster::OrderProcessor::Client->new(
        uri => $processor->uri,
        on_error => sub {
            $signal->send;
        },
        on_connect => sub {
            $_[0]->start($order->uuid);
        },
        service => {
            start => sub {
                (undef, $ok) = @_;
                $signal->send;
            }
        });
    $signal->wait;
    @o = ();

    if ($timeout) {
        $self->{logger}->warn('timeout starting order ', $order->uuid,' at order processor ', $processor->uri);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'timeout starting order'
                   ));
        return;
    }

    if (ref($ok)) {
        $self->{logger}->warn('error starting order ', $order->uuid,' at order processor ', $processor->uri, ':', $ok->to_string);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'error starting order: '.$ok->to_string
                   ));
        return;
    }

    unless ($ok == 1) {
        $self->{logger}->warn('unable to start order ', $order->uuid,' at order processor ', $processor->uri);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'start',
                       errno => 1,
                       errstr => 'unable to start order'
                   ));
        return;
    }

    $rpc->call($cli, 'start', 1);
    return;
}

=head2 function2

=cut

sub call_restart {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (exists $self->{order}->{$uuid}) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'order does not exist'
                   ));
        return;
    }
    
    my $order = $self->{order}->{$uuid};

    unless (defined $order->processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'order not assigned'
                   ));
        return;
    }

    my $processor = $self->{processor}->{$order->processor};

    unless (defined $processor) {
        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'lost processor of order'
                   ));
        return;
    }

    my (@o, $timeout, $signal, $ok);

    $timeout = 0;
    $signal = Coro::Signal->new;
    push @o, AnyEvent->timer(
        after => 30,
        cb => sub { $timeout = 1; $signal->send; });
    push @o, PollMonster::OrderProcessor::Client->new(
        uri => $processor->uri,
        on_error => sub {
            $signal->send;
        },
        on_connect => sub {
            $_[0]->restart($order->uuid);
        },
        service => {
            restart => sub {
                (undef, $ok) = @_;
                $signal->send;
            }
        });
    $signal->wait;
    @o = ();

    if ($timeout) {
        $self->{logger}->warn('timeout restarting order ', $order->uuid,' at order processor ', $processor->uri);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'timeout restarting order'
                   ));
        return;
    }

    if (ref($ok)) {
        $self->{logger}->warn('error restarting order ', $order->uuid,' at order processor ', $processor->uri, ':', $ok->to_string);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'error restarting order: '.$ok->to_string
                   ));
        return;
    }

    unless ($ok == 1) {
        $self->{logger}->warn('unable to restart order ', $order->uuid,' at order processor ', $processor->uri);

        $rpc->call($cli, PollMonster::RPC::Fault->new(
                       method => 'restart',
                       errno => 1,
                       errstr => 'unable to restart order'
                   ));
        return;
    }

    $rpc->call($cli, 'restart', 1);
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

=head2 function2

=cut

sub on_net_join {
    my ($self, $rpc, $cli, $uri) = @_;

    $rpc->push_sync_cli($cli, 'sync-orders', values %{$self->{order}});

    return;
}

=head2 function2

=cut

sub _setup_order {
    my ($self, $order) = @_;

    if (defined $order->processor) {
        my $processor;
        unless (defined ($processor = $self->{processor}->{$order->processor})) {
            $processor = $self->{processor}->{$order->processor} = PollMonster::OrderManager::Processor->new(
                uri => $order->processor
                );
        }

        $processor->add_order($order->uuid, $order);
    }
}

=head2 function2

=cut

sub call_sync_orders {
    my ($self, $rpc, $cli, @orders) = @_;

    foreach my $order (@orders) {
        unless (ref($order) eq 'HASH') {
            next;
        }
        unless (exists $self->{order}->{$order->{uuid}}) {
            $order = PollMonster::OrderManager::Order->new(%$order);

            if ($order->is_valid) {
                $self->{order}->{$order->uuid} = $order;
                $self->_setup_order($order);
            }
        }
    }

    return;
}

=head2 function2

=cut

sub call_sync_add {
    my ($self, $rpc, $cli, $order) = @_;

    unless (ref($order) eq 'HASH') {
        return;
    }

    unless (defined $order->{uuid}) {
        return;
    }

    if (exists $self->{order}->{$order->{uuid}}) {
        return;
    }

    $order = PollMonster::OrderManager::Order->new(%$order);

    if ($order->is_valid) {
        $self->{order}->{$order->uuid} = $order;
        $self->_setup_order($order);

        if (exists $order->option->{push} and $self->{rpc}->is_oldest) {
            $self->_order_push;
        }
    }

    return;
}

=head2 function2

=cut

sub call_sync_remove {
    my ($self, $rpc, $cli, $uuid) = @_;

    unless (defined $uuid) {
        return;
    }

    unless (exists $self->{order}->{$uuid}) {
        return;
    }

    my $order = delete $self->{order}->{$uuid};

    if (defined $order->processor and exists $self->{processor}->{$order->processor}) {
        $self->{processor}->{$order->processor}->del_order($order->uuid);
    }

    return;
}

=head2 function2

=cut

sub call_sync_update {
    my ($self, $rpc, $cli, $order) = @_;

    unless (ref($order) eq 'HASH') {
        return;
    }

    unless (defined $order->{uuid}) {
        return;
    }

    unless (exists $self->{order}->{$order->{uuid}}) {
        return;
    }

    my $order_obj = $self->{order}->{$order->{uuid}}->set_processor($order->{processor})
        ->set_module($order->{module})
        ->set_parameter($order->{parameter})
        ->set_option($order->{option});

    unless (defined $order->{processor}) {
        if (defined $order_obj->processor and exists $self->{processor}->{$order_obj->processor}) {
            $self->{processor}->{$order_obj->processor}->del_order($order_obj->uuid);
        }

        $order_obj->del_processor;
    }

    if ($order_obj->is_valid) {
        $self->_setup_order($order_obj);
    }
    else {
        delete $self->{order}->{$order_obj->uuid};

        if (defined $order_obj->processor and exists $self->{processor}->{$order_obj->processor}) {
            $self->{processor}->{$order_obj->processor}->del_order($order_obj->uuid);
        }
    }

    return;
}

=head2 function2

=cut

sub call_sync_remove_processor {
    my ($self, $rpc, $cli, $uri) = @_;

    unless (defined $uri) {
        return;
    }

    my $processor = delete $self->{processor}->{$uri};

    if (defined $processor) {
        foreach my $order (values %{$processor->order}) {
            $order->del_processor;
        }
    }

    return;
}

=head2 function2

=cut

sub _processors {
    my ($self) = @_;
    my (@o, @list, $timeout, $signal);

    unless ($self->is_registered and defined PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME)) {
        return;
    }

    unless (defined $self->{rpc} and $self->{rpc}->is_oldest) {
        return;
    }

    $signal = Coro::Signal->new;
    push @o, AnyEvent->timer(
        after => 30,
        cb => sub { $timeout = 1; $signal->send; });
    push @o, PollMonster::ProcessManager::Client->new(
        uri => PollMonster->CFG(GLOBAL_NAME, PROCESS_MANAGER_NAME),
        on_error => sub {
            $signal->send;
        },
        on_connect => sub {
            $_[0]->list(ORDER_PROCESSOR_NAME);
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
        $self->{logger}->warn('timeout retriving order processors');
        return;
    }

    if (%{$self->{processor}}) {
        my %processor = map { $_->{uri} => $_ } @list;

        foreach my $processor (values %{$self->{processor}}) {
            unless (exists $processor{$processor->uri}) {
                delete $self->{processor}->{$processor->uri};

                foreach my $order (values %{$processor->order}) {
                    $order->del_processor;
                    $self->{rpc}->push_sync('sync-update', $order);
                }

                $self->{rpc}->push_sync('sync-remove-processor', $processor->uri);

                $self->{logger}->info('lost processor ', $processor->uri);
            }
        }
    }

    foreach my $entry (@list) {
        unless (exists $self->{processor}->{$entry->{uri}}) {
            my $processor = PollMonster::OrderManager::Processor->new(
                uri => $entry->{uri}
                );

            $self->{logger}->info('new order processor ', $processor->uri, ', getting orders');

            my @orders = ();
            $timeout = 0;
            $signal = Coro::Signal->new;
            push @o, AnyEvent->timer(
                after => 30,
                cb => sub { $timeout = 1; $signal->send; });
            push @o, PollMonster::OrderProcessor::Client->new(
                uri => $processor->uri,
                on_error => sub {
                    $signal->send;
                },
                on_connect => sub {
                    $_[0]->list();
                },
                service => {
                    list => sub {
                        (undef, @orders) = @_;
                        $signal->send;
                    }
                });
            $signal->wait;
            @o = ();

            if ($timeout) {
                $self->{logger}->warn('timeout getting orders from order processor ', $processor->uri);
                next;
            }

            $self->{processor}->{$processor->uri} = $processor;

            foreach my $order (@orders) {
                unless (exists $self->{order}->{$order->{uuid}}) {
                    $order = PollMonster::OrderManager::Order->new(%$order);

                    if ($order->is_valid) {
                        $self->{order}->{$order->{uuid}} = $order;
                        $self->{rpc}->push_sync('sync-add', $order);
                    }
                }
                else {
                    $order = $self->{order}->{$order->{uuid}};
                }

                $order->set_processor($processor->uri);
                $processor->add_order($order->uuid, $order);
            }

            $self->{logger}->info('new order processor ', $processor->uri, ' (',$processor->num_orders,' orders)');
        }
    }

    $self->_order_push;

    return;
}

sub _order_push {
    my ($self) = @_;
    my (@o, $timeout, $signal);
    my $g = $self->{order_push_semaphore}->guard;

    if (%{$self->{order}} and %{$self->{processor}}) {
        foreach my $order (values %{$self->{order}}) {
            unless (defined $order->processor) {
                my ($processor) = sort { $a->num_orders <=> $b->num_orders } values %{$self->{processor}};

                my $ok;
                $timeout = 0;
                $signal = Coro::Signal->new;
                push @o, AnyEvent->timer(
                    after => 30,
                    cb => sub { $timeout = 1; $signal->send; });
                push @o, PollMonster::OrderProcessor::Client->new(
                    uri => $processor->uri,
                    on_error => sub {
                        $signal->send;
                    },
                    on_connect => sub {
                        $_[0]->add($order->uuid, $order->module, $order->parameter, $order->option);
                    },
                    service => {
                        add => sub {
                            (undef, $ok) = @_;
                            $signal->send;
                        }
                    });
                $signal->wait;
                @o = ();

                if ($timeout) {
                    $self->{logger}->warn('timeout sending order ', $order->uuid,' to order processor ', $processor->uri);
                    next;
                }

                if (ref($ok)) {
                    $self->{logger}->warn('error sending order ', $order->uuid,' to order processor ', $processor->uri, ':', $ok->to_string);
                    next;
                }

                unless ($ok == 1) {
                    $self->{logger}->warn('unable to add order ', $order->uuid,' to order processor ', $processor->uri);
                    next;
                }

                $order->set_processor($processor->uri);
                $processor->add_order($order->uuid, $order);

                $self->{rpc}->push_sync('sync-update', $order);

                $self->{logger}->info('added order ', $order->uuid,' to order processor ', $processor->uri);
            }
        }
    }

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
