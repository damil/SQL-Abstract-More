use strict;
use warnings;
no warnings qw/qw/;
use Test::More;

use SQL::Abstract::More;
use SQL::Abstract::Test import => ['is_same_sql_bind'];


plan tests => 1;

my $sqla  = SQL::Abstract::More->new(
    quote_char   => '`',
    #quote_char   => ['[',']'],
    name_sep     => '!',
    #escape_char => '`',
);
my ($sql, @bind);


($sql, @bind) = $sqla->select(
    -columns => [ 'A!col_A', 'B!col_B|b' ],
    -from    => [-join => qw/A fk=pk B /],
    -where    => {
       'A!foo' => 'a',
       'b!foo' => 'b',
    },
);
is_same_sql_bind(
  $sql, \@bind,
  "SELECT `A`!`col_A`, `B`!`col_B` AS `b` FROM `A` INNER JOIN `B` ON ( `A`!`fk` = `B`!`pk` ) WHERE ( ( `A`!`foo` = ? AND `b`!`foo` = ? ) )",
  ['a', 'b'],
);
