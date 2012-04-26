use strict;

use PollMonster::RPC::Client;

use EV;
use AnyEvent;
use Coro;
use Coro::EV;
use Coro::AnyEvent;

use Log::Log4perl;

use Time::HiRes;
use Coro::Debug;
use Coro::Signal;
use Coro::Timer;

Log::Log4perl->init( \q(
log4perl.logger                        = DEBUG, PollMonster
log4perl.appender.PollMonster          = Log::Log4perl::Appender::Screen
log4perl.appender.PollMonster.stderr   = 0
log4perl.appender.PollMonster.layout   = Log::Log4perl::Layout::PatternLayout
log4perl.appender.PollMonster.layout.ConversionPattern = %d [%R] %H %l [%P] %p: %m%n
) );

my ($in_count,$out_count,$in_last,$out_last,$time,$err_count);

async {
    EV::loop;
}

my $signal = Coro::Signal->new;

my $cli = PollMonster::RPC::Client->new(
    uri => shift(@ARGV),
    on_connect => sub {$signal->send}
    );

$signal->wait;

exit -1 unless ($cli->is_connected);

my @work;
push @work, 'work' for(1..80);

my $timer; $timer = AnyEvent->timer(
    after => 1,
    interval => 1,
    cb => sub {
        my ($in, $out, $slept);

        $slept = Time::HiRes::time() - $time;

        $in = $in_count - $in_last;
        $out = $out_count - $out_last;
        $in_last += $in;
        $out_last += $out;
        if($slept < 1.0)
        {
            $in += $in * (1.0 - $slept);
        }
        elsif($slept > 1.0)
        {
            $in /= $slept;
        }
        print 'in rate: ', $in, '/s  out rate: ', $out, '/s  errors: ', $err_count, "\n";

        $time = Time::HiRes::time();
    });

my $cv = AnyEvent->condvar;

    my $sig_int; $sig_int = AnyEvent->signal(
        signal => 'INT',
        cb => sub { $cv->send; }
        );

my $coro = async {
    $Coro::current->prio(-2);
while () {
    $cli->call('work', @work);
    $out_count++;
    #cede if (!($out_count % 10));
}
};

$cv->recv;
$coro->cancel;

#    while (1) {
#        $cli->call('work', @work);
#        $out_count++;
#    }
