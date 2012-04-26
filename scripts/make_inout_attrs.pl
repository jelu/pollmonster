use strict;
use warnings;

while(<>) {
    chomp();

    if (/my \@_(\S+);/o) {
        my $attr = $1;

        next if ($attr =~ /obj_reclaim/o);

        print
"    sub $attr {
        \$_${attr}[\${\$_[0]}];
    }

    sub get_$attr {
        \$_${attr}[\${\$_[0]}];
    }

    sub set_$attr {
        \$_${attr}[\${\$_[0]}] = \$_[1] if (defined \$_[1]);

        \$_[0];
    }

";
    }

}
