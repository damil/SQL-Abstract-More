use strict;
use warnings;
use Test::More;
use SQL::Abstract::Test import => [qw/is_same_sql_bind/];

require SQL::Abstract::More; # no "use" ... so no import()

my $sqla = SQL::Abstract::More->new; # import() called implicitly through new()

ok $sqla->isa('SQL::Abstract::Classic'), '@ISA was populated';

my ($sql, @bind) = $sqla->select(
  -columns  => [qw/bar/],
  -from     => 'Foo',
  -where    => {bar => {">" => 123}},
  -order_by => ['bar']
);
is_same_sql_bind(
  $sql, \@bind,
  "SELECT bar FROM Foo WHERE bar > ? ORDER BY bar", [123],
  "select",
);


done_testing;


