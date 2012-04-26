use strict;
use warnings;

my $type = shift(@ARGV);

unless ($type eq 'a' or $type eq 'h') {
    unshift(@ARGV, $type);
    $type = '';
}

while(my $attr = shift(@ARGV)) {

    if ($type eq 'a') {
        print
"sub $attr {
    \$_[0]->{$attr};
}

sub add_$attr {
    my \$self = shift;

    foreach (\@_) {
        push(\@{\$self->{$attr}}, \$_) if (defined \$_);
    }

    \$self;
}

sub set_$attr {
    \$_[0]->{$attr} = \$_[1] if (defined \$_[1]);

    \$_[0];
}

";
    }
    elsif ($type eq 'h') {
        print
"sub $attr {
    \$_[0]->{$attr};
}

sub add_$attr {
    \$_[0]->{$attr}->{\$_[1]} = \$_[2] if (defined \$_[1] and defined \$_[2]);

    \$_[0];
}

sub del_$attr {
    return delete \$_[0]->{$attr}->{\$_[1]} if (defined \$_[1]);

    return;
}

sub set_$attr {
    \$_[0]->{$attr} = \$_[1] if (defined \$_[1]);

    \$_[0];
}

";
    }
    else {
        print
"sub $attr {
    \$_[0]->{$attr};
}

sub set_$attr {
    \$_[0]->{$attr} = \$_[1] if (defined \$_[1]);

    \$_[0];
}

";
}
}
