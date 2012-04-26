use strict;
use warnings;

while(<>) {
    chomp();

    if (/sub\s+_(\S+)\s+\(\)\{/o) {
        my $attr = $1;

        print
"sub $attr {
    \$_[0]->[_$attr];
}

sub get_$attr {
    \$_[0]->[_$attr];
}

sub set_$attr {
    \$_[0]->[_$attr] = \$_[1] if (defined \$_[1]);

    \$_[0];
}

";
    }

}
