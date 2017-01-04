use strict;
use warnings;
use Test::More;
use SQL::Abstract::More;

my $sqla = SQL::Abstract::More->new;

my ( $sql, @bind ) = $sqla->select(
	-columns  => [qw( foo ) ],
	-from     => [ 'bar' ],
	-where    => { 'bar.foo' => 1 },
	-order_by => [ '-foo'],
);

is $sql, 'SELECT foo FROM bar WHERE ( bar.foo = ? ) ORDER BY foo DESC', 'SQL';
is_deeply \@bind, [ 1 ], 'bind params';

done_testing;
