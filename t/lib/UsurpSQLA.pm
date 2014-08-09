package UsurpSQLA;

use Filter::Simple sub {s/SQL::Abstract(;|->)/SQL::Abstract::More$1/g;};

1;


