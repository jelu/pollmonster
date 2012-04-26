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

    sub add_$attr {
        my \$self = shift;

        foreach (\@_) {
            push(\@{\$_${attr}[\${\$self}]}, \$_) if (defined \$_);
        }

        \$self;
    }

    sub set_$attr {
        \$_${attr}[\${\$_[0]}] = \$_[1] if (ref(\$_[1]) eq 'ARRAY');

        \$_[0];
    }

";
    }

}
