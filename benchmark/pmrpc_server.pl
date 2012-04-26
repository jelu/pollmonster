use strict;

use PollMonster::RPC::Server;

use EV;
use AnyEvent;
use Coro;
use Coro::EV;
use Coro::AnyEvent;

use Log::Log4perl;

use Time::HiRes;
use Coro::Debug;

Log::Log4perl->init( \q(
log4perl.logger                        = DEBUG, PollMonster
log4perl.appender.PollMonster          = Log::Log4perl::Appender::Screen
log4perl.appender.PollMonster.stderr   = 0
log4perl.appender.PollMonster.layout   = Log::Log4perl::Layout::PatternLayout
log4perl.appender.PollMonster.layout.ConversionPattern = %d [%R] %H %l [%P] %p: %m%n
) );

my ($in_count,$out_count,$in_last,$out_last,$time,$err_count);

my $srv = PollMonster::RPC::Server->new(
    uri => 'tcp://127.0.0.1',
    name => 'benchmark',
    prepare => sub { print "tcp://$_[2]:$_[3]\n"; },
    service => {
        work => sub {
            $in_count++;
        }
    });

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

EV::loop;
