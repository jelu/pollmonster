#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'PollMonster' ) || print "Bail out!
";
}

diag( "Testing PollMonster $PollMonster::VERSION, Perl $], $^X" );
