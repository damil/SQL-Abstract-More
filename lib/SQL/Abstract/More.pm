package SQL::Abstract::More;
use strict;
use warnings;

use parent 'SQL::Abstract';
use MRO::Compat;
use mro 'c3'; # implements next::method

use Params::Validate  qw/validate SCALAR SCALARREF CODEREF ARRAYREF HASHREF
                                  UNDEF  BOOLEAN/;
use Scalar::Util      qw/reftype/;
use Carp;
use namespace::autoclean;

our $VERSION = '1.01';

# builtin methods for "Limit-Offset" dialects
my %limit_offset_dialects = (
  LimitOffset => sub {my ($self, $limit, $offset) = @_;
                      $offset ||= 0;
                      return "LIMIT ? OFFSET ?", $limit, $offset;},
  LimitXY     => sub {my ($self, $limit, $offset) = @_;
                      $offset ||= 0;
                      return "LIMIT ?, ?", $offset, $limit;},
  LimitYX     => sub {my ($self, $limit, $offset) = @_;
                      $offset ||= 0;
                      return "LIMIT ?, ?", $limit, $offset;},
  RowNum      => sub {
    my ($self, $limit, $offset) = @_;
    # HACK below borrowed from SQL::Abstract::Limit. Not perfect, though,
    # because it brings back an additional column. Should borrow from 
    # DBIx::Class::SQLMaker::LimitDialects, which does the proper job ...
    # but it says : "!!! THIS IS ALSO HORRIFIC !!! /me ashamed"; so
    # I'll only take it as last resort; still exploring other ways.
    # Probably the proper way would be to go through Oracle scrollable cursors.
    my $sql = "SELECT * FROM ("
            .   "SELECT subq_A.*, ROWNUM rownum__index FROM (%s) subq_A "
            .   "WHERE ROWNUM <= ?"
            .  ") subq_B WHERE rownum__index >= ?";

    no warnings 'uninitialized'; # in case $limit or $offset is undef
    # row numbers start at 1
    return $sql, $offset + $limit + 1, $offset + 1;
  },
 );

# builtin join operators with associated sprintf syntax
my %common_join_syntax = (
  '<=>' => '%s INNER JOIN %s ON %s',
   '=>' => '%s LEFT OUTER JOIN %s ON %s',
  '<='  => '%s RIGHT JOIN %s ON %s',
  '=='  => '%s NATURAL JOIN %s',
);
my %right_assoc_join_syntax = %common_join_syntax;
s/JOIN %s/JOIN (%s)/ foreach values %right_assoc_join_syntax;

# specification of parameters accepted by the new() method
my %params_for_new = (
  table_alias      => {type => SCALAR|CODEREF, default  => '%s AS %s'},
  column_alias     => {type => SCALAR|CODEREF, default  => '%s AS %s'},
  limit_offset     => {type => SCALAR|CODEREF, default  => 'LimitOffset'},
  join_syntax      => {type => HASHREF,        default  =>
                                                    \%common_join_syntax},
  join_assoc_right => {type => BOOLEAN,        default  => 0},
  max_members_IN   => {type => SCALAR,         optional => 1},
  sql_dialect      => {type => SCALAR,         optional => 1},
);

# builtin collection of parameters, for various databases
my %sql_dialects = (
 MsAccess  => { join_assoc_right => 1,
                join_syntax      => \%right_assoc_join_syntax},
 BasisJDBC => { column_alias     => "%s %s",
                max_members_IN   => 255                      },
 MySQL_old => { limit_offset     => "LimitXY"                },
 Oracle    => { limit_offset     => "RowNum",
                max_members_IN   => 999,
                table_alias      => '%s %s',
                column_alias     => '%s %s',                 },
);

# specification of parameters accepted by select, insert, update, delete
my %params_for_select = (
  -columns      => {type => SCALAR|ARRAYREF,         default  => '*'},
  -from         => {type => SCALAR|SCALARREF|ARRAYREF},
  -where        => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  -group_by     => {type => SCALAR|ARRAYREF,         optional => 1},
  -having       => {type => SCALAR|ARRAYREF|HASHREF, optional => 1,
                                                     depends  => '-group_by'},
  -order_by     => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  -page_size    => {type => SCALAR,                  optional => 1},
  -page_index   => {type => SCALAR,                  optional => 1,
                                                     depends  => '-page_size'},
  -limit        => {type => SCALAR,                  optional => 1},
  -offset       => {type => SCALAR,                  optional => 1,
                                                     depends  => '-limit'},
  -for          => {type => SCALAR|UNDEF,            optional => 1},
  -want_details => {type => BOOLEAN,                 optional => 1},
);
my %params_for_insert = (
  -into         => {type => SCALAR},
  -values       => {type => SCALAR|ARRAYREF|HASHREF},
);
my %params_for_update = (
  -table        => {type => SCALAR},
  -set          => {type => HASHREF},
  -where        => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
);
my %params_for_delete = (
  -from         => {type => SCALAR},
  -where        => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
);


#----------------------------------------------------------------------
# object creation
#----------------------------------------------------------------------

sub new {
  my ($class, %params) = @_;

  # extract params for this subclass
  my %more_params;
  foreach my $key (keys %params_for_new) {
    $more_params{$key} = delete $params{$key} if exists $params{$key};
  }

  # import params from SQL dialect, if any
  my $dialect = delete $more_params{sql_dialect};
  if ($dialect) {
    my $dialect_params = $sql_dialects{$dialect}
      or die "no such sql dialect: $dialect";
    $more_params{$_} ||= $dialect_params->{$_} foreach keys %$dialect_params;
  }

  # check parameters
  my @more_params = %more_params;
  my $more_self   = validate(@more_params, \%params_for_new);

  # call parent constructor
  my $self = $class->next::method(%params);

  # inject into $self
  $self->{$_} = $more_self->{$_} foreach keys %$more_self;

  # arguments supplied as scalars are transformed into coderefs
  ref $self->{column_alias} or $self->_make_AS_through_sprintf('column_alias');
  ref $self->{table_alias}  or $self->_make_AS_through_sprintf('table_alias');
  ref $self->{limit_offset} or $self->_choose_LIMIT_OFFSET_dialect;

  # regex for parsing join specifications
  my $join_ops = join '|', map quotemeta, keys %{$self->{join_syntax}};
  $self->{join_regex} = qr[
     ^              # initial anchor 
     ($join_ops)?   # $1: join operator (i.e. '<=>', '=>', etc.))
     ([[{])?        # $2: opening '[' or '{'
     (.*?)          # $3: content of brackets
     []}]?          # closing ']' or '}'
     $              # final anchor
   ]x;

  return $self;
}

#----------------------------------------------------------------------
# the select method
#----------------------------------------------------------------------

sub select {
  my $self = shift;

  # if got positional args, this is not our job, just delegate to the parent
  return $self->next::method(@_) if !&_called_with_named_args;

  # declare variables and parse arguments;
  my ($join_info, %aliased_columns);
  my %args = validate(@_, \%params_for_select);

  # compute join info if the datasource is a join
  if (ref $args{-from} eq 'ARRAY' && $args{-from}[0] eq '-join') {
    my @join_args = @{$args{-from}};
    shift @join_args;           # drop initial '-join'
    $join_info   = $self->join(@join_args);
    $args{-from} = \($join_info->{sql});
  }

  # reorganize columns; initial members starting with "-" are extracted
  # into a separate list @post_select, later re-injected into the SQL
  my @cols = ref $args{-columns} ? @{$args{-columns}} : $args{-columns};
  my @post_select;
  push @post_select, shift @cols while @cols && $cols[0] =~ s/^-//;
  foreach my $col (@cols) {
    # extract alias, if any (recognized as "column|alias")
    ($col, my $alias) = split /\|/, $col, 2;
    if ($alias) {
      $aliased_columns{$alias} = $col;
      $col = $self->column_alias($col, $alias);
    }
  }
  $args{-columns} = \@cols;

  # reorganize pagination
  if ($args{-page_index} || $args{-page_size}) {
    not exists $args{$_} or croak "-page_size conflicts with $_"
      for qw/-limit -offset/;
    $args{-limit} = $args{-page_size};
    if ($args{-page_index}) {
      $args{-offset} = ($args{-page_index} - 1) * $args{-page_size};
    }
  }

  # within -order_by, translate +/- prefixes into SQL ASC/DESC
  $args{-order_by} = $self->_reorganize_order($args{-order_by})
    if $args{-order_by};

  # generate initial ($sql, @bind)
  my @old_API_args = @args{qw/-from -columns -where -order_by/};
  my ($sql, @bind) = $self->next::method(@old_API_args);
  unshift @bind, @{$join_info->{bind}} if $join_info;

  # add @post_select clauses if needed (for ex. -distinct)
  my $post_select = join " ", @post_select;
  $sql =~ s[^SELECT ][SELECT $post_select ]i if $post_select;

  # add GROUP BY/HAVING if needed
  if ($args{-group_by}) {
    my $grp = $self->_reorganize_order($args{-group_by});
    my $sql_grp = $self->where(undef, $grp);
    $sql_grp =~ s/\bORDER\b/GROUP/;
    if ($args{-having}) {
      my ($sql_having, @bind_having) = $self->where($args{-having});
      $sql_having =~ s/\bWHERE\b/HAVING/;
      $sql_grp .= " $sql_having";
      push @bind, @bind_having;
    }
    $sql =~ s[ORDER BY|$][$sql_grp $&]i;
  }

  # add LIMIT/OFFSET if needed
  if ($args{-limit}) {
    my ($limit_sql, @limit_bind) 
      = $self->limit_offset(@args{qw/-limit -offset/});
    $sql = $limit_sql =~ /%s/ ? sprintf $limit_sql, $sql
                              : "$sql $limit_sql";
    push @bind, @limit_bind;
  }

  # add FOR if needed
  $sql .= " FOR $args{-for}" if $args{-for};

  if ($args{-want_details}) {
    return {sql             => $sql,
            bind            => \@bind,
            aliased_tables  => ($join_info && $join_info->{aliased_tables}),
            aliased_columns => \%aliased_columns          };
  }
  else {
    return ($sql, @bind);
  }
}

#----------------------------------------------------------------------
# insert, update and delete methods
#----------------------------------------------------------------------

sub insert {
  my $self = shift;

  my @old_API_args;
  if (&_called_with_named_args) {
    my %args = validate(@_, \%params_for_insert);
    @old_API_args = @args{qw/-into -values/};
    push @old_API_args, {returning => $args{-returning}} if $args{-returning};
  }
  else {
    @old_API_args = @_;
  }

  return $self->next::method(@old_API_args);
}


sub update {
  my $self = shift;

  my @old_API_args;
  if (&_called_with_named_args) {
    my %args = validate(@_, \%params_for_update);
    @old_API_args = @args{qw/-table -set -where/};
  }
  else {
    @old_API_args = @_;
  }

  return $self->next::method(@old_API_args);
}



sub delete {
  my $self = shift;

  my @old_API_args;
  if (&_called_with_named_args) {
    my %args = validate(@_, \%params_for_delete);
    @old_API_args = @args{qw/-from -where/};
  }
  else {
    @old_API_args = @_;
  }

  return $self->next::method(@old_API_args);
}



#----------------------------------------------------------------------
# other public methods
#----------------------------------------------------------------------

# same pattern for 3 invocation methods
foreach my $attr (qw/table_alias column_alias limit_offset/) {
  no strict 'refs';
  *{$attr} = sub {
    my $self = shift;
    my $method = $self->{$attr}; # grab reference to method body
    $self->$method(@_);          # invoke
  };
}

# invocation method for 'join'
sub join {
  my $self = shift;

  # start from the right if right-associative
  @_ = reverse @_ if $self->{join_assoc_right};

  # shift first single item (a table) before reducing pairs (op, table)
  my $combined = shift;
  $combined    = $self->_parse_table($combined)      unless ref $combined;

  # reduce pairs (op, table)
  while (@_) {
    # shift 2 items : next join specification and next table
    my $join_spec  = shift;
    my $table_spec = shift or die "join(): improper number of operands";

    $join_spec  = $self->_parse_join_spec($join_spec) unless ref $join_spec;
    $table_spec = $self->_parse_table($table_spec)    unless ref $table_spec;
    $combined   = $self->_single_join($combined, $join_spec, $table_spec);
  }

  return $combined; # {sql=> .., bind => [..], aliased_tables => {..}}
}


# utility for merging several "where" clauses
sub merge_conditions {
  my $self = shift;
  my %merged;

  foreach my $cond (@_) {
    my $reftype = reftype($cond) || '';
    if    ($reftype eq 'HASH')  {
      foreach my $col (keys %$cond) {
        $merged{$col} = $merged{$col} ? [-and => $merged{$col}, $cond->{$col}]
                                      : $cond->{$col};
      }
    }
    elsif ($reftype eq 'ARRAY') {
      $merged{-nest} = $merged{-nest} ? {-and => [$merged{-nest}, $cond]}
                                      : $cond;
    }
    elsif ($cond) {
      $merged{$cond} = \"";
    }
  }
  return \%merged;
}



#----------------------------------------------------------------------
# private utility methods for 'select'
#----------------------------------------------------------------------

sub _reorganize_order {
  my ($self, $order) = @_;

  # force into an arrayref
  $order = [$order] if not ref $order;

  # '-' and '+' prefixes are translated into {-desc/asc => } hashrefs
  foreach my $item (@$order) {
    next if ref $item;
    $item =~ s/^-//  and $item = {-desc => $item} and next;
    $item =~ s/^\+// and $item = {-asc  => $item};
  }

  return $order;
}



#----------------------------------------------------------------------
# private utility methods for 'join'
#----------------------------------------------------------------------

sub _parse_table {
  my ($self, $table) = @_;

  # extract alias, if any (recognized as "table|alias")
  ($table, my $alias) = split /\|/, $table, 2;

  # build a table spec
  return {
    sql            => $self->table_alias($table, $alias),
    bind           => [],
    name           => ($alias || $table),
    aliased_tables => {$alias ? ($alias => $table) : ()},
   };
}


sub _parse_join_spec {
  my ($self, $join_spec) = @_;

  # parse the join specification
  $join_spec
    or die "empty join specification";
  my ($op, $bracket, $cond_list) = ($join_spec =~ $self->{join_regex})
    or die "incorrect join specification : $join_spec\n$self->{join_regex}";
  $op        ||= '<=>';
  $bracket   ||= '{';
  $cond_list ||= '';

  # find join syntax corresponding to the join operator
  $self->{join_syntax}{$op}
    or die "unknown join operator : $op";

  # accumulate conditions as pairs ($left => \"$op $right")
  my @conditions;
  foreach my $cond (split /,/, $cond_list) {
    # parse the comparison operator (left and right operands + cmp op)
    my ($left, $cmp, $right) = split /([<>=!^]{1,2})/, $cond
      or die "can't parse join condition: $cond";

    # if operands are not qualified by table/alias name, add placeholders
    $left  = "%1\$s.$left"  unless $left  =~ /\./;
    $right = "%2\$s.$right" unless $right =~ /\./;

    # add this pair into the list
    push @conditions, $left, \"$cmp $right";
  }

  # list becomes an arrayref or hashref (for SQLA->where())
  my $join_on = $bracket eq '[' ? [@conditions] : {@conditions};

  # return a new join spec
  return {operator  => $op,
          condition => $join_on};
}

sub _single_join {
  my $self = shift;

  # if right-associative, restore proper left-right order in pair
  @_ = reverse @_ if $self->{join_assoc_right};
  my ($left, $join_spec, $right) = @_;

  # compute the "ON" clause (assuming it contains '%1$s', '%2$s' for
  # left/right tables)
  my ($sql, @bind) = $self->where($join_spec->{condition});
  $sql =~ s/^\s*WHERE\s+//;
  $sql = sprintf $sql, $left->{name}, $right->{name};

  # assemble all elements
  my $syntax = $self->{join_syntax}{$join_spec->{operator}};
  $sql = sprintf $syntax, $left->{sql}, $right->{sql}, $sql;
  unshift @bind, @{$left->{bind}}, @{$right->{bind}};

  # build result and return
  my %result = (sql => $sql, bind => \@bind);
  $result{name}    = ($self->{join_assoc_right} ? $left : $right)->{name};
  $result{aliased_tables} = $left->{aliased_tables};
  foreach my $alias (keys %{$right->{aliased_tables}}) {
    $result{aliased_tables}{$alias} = $right->{aliased_tables}{$alias};
  }

  return \%result;
}


#----------------------------------------------------------------------
# override of parent's "_where_field_IN"
#----------------------------------------------------------------------

sub _where_field_IN {
  my ($self, $k, $op, $vals) = @_;

  my $max_members_IN = $self->{max_members_IN};
  if ($max_members_IN && reftype $vals eq 'ARRAY' 
                      &&  @$vals > $max_members_IN) {
    my @vals = @$vals;
    my @slices;
    while (my @slice = splice(@vals, 0, $max_members_IN)) {
      push @slices, \@slice;
    }
    my @clauses = map {{-$op, $_}} @slices;
    my $connector = $op =~ /^not/i ? '-and' : '-or';
    unshift @clauses, $connector;
    my ($sql, @bind) = $self->where({$k => \@clauses});
    $sql =~ s/\s*where\s*\((.*)\)/$1/i;
    return ($sql, @bind);
  }
  else {
    return $self->next::method($k, $op, $vals);
  }
}



#----------------------------------------------------------------------
# method creations through closures
#----------------------------------------------------------------------

sub _make_AS_through_sprintf {
  my ($self, $attribute) = @_;
  my $syntax = $self->{$attribute};
  $self->{$attribute} = sub {
    my ($self, $name, $alias) = @_;
    return $alias ? sprintf($syntax, $name, $alias) : $name;
  };
}

sub _choose_LIMIT_OFFSET_dialect {
  my $self = shift;
  my $dialect = $self->{limit_offset};
  my $method = $limit_offset_dialects{$dialect}
    or die "no such limit_offset dialect: $dialect";
  $self->{limit_offset} = $method;
};


#----------------------------------------------------------------------
# utility to decide if the method was called with named or positional args
#----------------------------------------------------------------------

sub _called_with_named_args {
  return $_[0] && !ref $_[0]  && substr($_[0], 0, 1) eq '-';
}


1; # End of SQL::Abstract::More

__END__

=head1 NAME

SQL::Abstract::More - extension of SQL::Abstract with more constructs and more flexible API

=head1 DESCRIPTION

Generates SQL from Perl datastructures.  This is a subclass of
L<SQL::Abstract>, fully compatible with the parent class, but it
handles a few additional SQL constructs, and provides a different API
with named parameters instead of positional parameters, so that
various SQL fragments are more easily identified.

This module was designed for the specific needs of
L<DBIx::DataModel>, but is published as a standalone distribution,
because it may possibly be useful for other needs.

=head1 SYNOPSIS

  my $sqla = SQL::Abstract::More->new();
  my ($sql, @bind);

  ($sql, @bind) = $sqla->select(
   -columns  => [-distinct => qw/col1 col2/],
   -from     => 'Foo',
   -where    => {bar => {">" => 123}},
   -order_by => [qw/col1 -col2 +col3/],  # BY col1, col2 DESC, col3 ASC
   -limit    => 100,
   -offset   => 300,
  );

  ($sql, @bind) = $sqla->select(
    -columns => [         qw/Foo.col_A|a           Bar.col_B|b /],
    -from    => [-join => qw/Foo           fk=pk   Bar         /],
  );

  my $merged = $sqla->merge_conditions($cond_A, $cond_B, ...);
  ($sql, @bind) = $sqla->select(..., -where => $merged, ..);

  ($sql, @bind) = $sqla->insert(
    -into   => $table,
    -values => {col => $val, ...},
  );
  ($sql, @bind) = $sqla->update(
    -table => $table,
    -set   => {col => $val, ...},
    -where => \%conditions,
  );
  ($sql, @bind) = $sqla->delete (
    -from  => $table
    -where => \%conditions,
  );

=head1 CLASS METHODS

=head2 new

  my $sqla = SQL::Abstract::More->new(%options);

where C<%options> may contain any of the options for the parent
class (see L<SQL::Abstract/new>), plus the following :

=over

=item table_alias

A L<sprintf> format description for generating table aliasing clauses.
The default is C<%s AS %s>.
Can also be supplied as a method coderef (see L</"Overriding methods">).

=item column_alias

A L<sprintf> format description for generating column aliasing clauses.
The default is C<%s AS %s>.
Can also be supplied as a method coderef.

=item limit_offset

Name of a "limit-offset dialect", which can be one of
C<LimitOffset>, C<LimitXY>, C<LimitYX> or C<RowNum>; 
see L<SQL::Abstract::Limit> for an explation of those dialects.
Here, unlike the L<SQL::Abstract::Limit> implementation,
limit and offset values are treated as regular values,
with placeholders '?' in the SQL; values are postponed to the
C<@bind> list.

The argument can also be a coderef (see below
L</"Overriding methods">). That coderef takes C<$self, $limit, $offset>
as arguments, and should return C<($sql, @bind)>. If C<$sql> contains
C<%s>, it is treated as a L<sprintf> format string, where the original
SQL is injected into C<%s>.


=item join_syntax

A hashref where keys are abreviations for join
operators to be used in the L</join> method, and 
values are associated SQL clauses with placeholders
in L<sprintf> format. The default is described
below under the L</join> method.

=item join_assoc_right

A boolean telling if multiple joins should be associative 
on the right or on the left. Default is false (i.e. left-associative).

=item max_members_IN

An integer specifying the maximum number of members in a "IN" clause.
If the number of given members is greater than this maximum, 
C<SQL::Abstract::More> will automatically split it into separate
clauses connected by 'OR' (or connected by 'AND' if used with the
C<-not_in> operator).

  my $sqla = SQL::Abstract::More->new(max_members_IN => 3);
  ($sql, @bind) = $sqla->select(
   -from     => 'Foo',
   -where    => {foo => {-in     => [1 .. 5]}},
                 bar => {-not_in => [6 .. 10]}},
  );
  # .. WHERE (     (foo IN (?,?,?) OR foo IN (?, ?))
  #            AND (bar NOT IN (?,?,?) AND bar NOT IN (?, ?)) )


=item sql_dialect

This is actually a "meta-argument" : it injects a collection
of regular arguments, tuned for a specific SQL dialect.
Dialects implemented so far are :

=over

=item MsAccess

For Microsoft Access. Overrides the C<join> syntax to be right-associative.

=item BasisJDBC

For Livelink Collection Server (formerly "Basis"), accessed
through a JDBC driver. Overrides the C<column_alias> syntax.
Sets C<max_members_IN> to 255.

=item MySQL_old

For old versions of MySQL. Overrides the C<limit_offset> syntax.
Recent versions of MySQL do not need that because they now
implement the regular "LIMIT ? OFFSET ?" ANSI syntax.

=item Oracle

For Oracle. Overrides the C<limit_offset> to use the "RowNum" dialect
(beware, this injects an additional column C<rownum__index> into your
resultset). Also sets C<max_members_IN> to 999.

=back

=back

=head3 Overriding methods

Several arguments to C<new()> can be references to method
implementations instead of plain scalars : this allows you to
completely redefine a behaviour without the need to subclass.  Just
supply a regular method body as a code reference : for example, if you
need another implementation for LIMIT-OFFSET, you could write

  my $sqla = SQL::Abstract::More->new(
    limit_offset => sub {
      my ($self, $limit, $offset) = @_;
      defined $limit or die "NO LIMIT!"; #:-)
      $offset ||= 0;
      my $last = $offset + $limit;
      return ("ROWS ? TO ?", $offset, $last); # ($sql, @bind)
     });


=head1 INSTANCE METHODS

=head2 select

  # positional parameters, directly passed to the parent class
  ($sql, @bind) = $sqla->select($table, $columns, $where, $order);

  # named parameters, handled in this class 
  ($sql, @bind) = $sqla->select(
    -columns  => \@columns,
      # OR: -columns => [-distinct => @columns],
    -from     => $table || \@joined_tables,
    -where    => \%where,
    -order_by => \@order,
    -group_by => \@group_by,
    -having   => \%having_criteria,
    -limit => $limit, -offset => $offset,
      # OR: -page_size => $size, -page_index => $index,
    -for      => $purpose,
   );

  my $details = $sqla->select(..., want_details => 1);
  # keys in %$details: sql, bind, aliased_tables, aliased_columns

If called with positional parameters, as in L<SQL::Abstract>, 
C<< select() >> just forwards the call to the parent class. Otherwise, if
called with named parameters, as in the example above, some additional
SQL processing is performed.

The following named arguments can be specified :

=over

=item C<< -columns => \@columns >> 

C<< \@columns >>  is a reference to an array
of SQL column specifications (i.e. column names, 
C<*> or C<table.*>, functions, etc.).

A '|' in a column is translated into a column aliasing clause:
this is convenient when
using perl C<< qw/.../ >> operator for columns, as in

  -columns => [ qw/table1.longColumn|t1lc table2.longColumn|t2lc/ ]

SQL column aliasing is then generated through the L</column_alias> method.

Initial items in C<< @columns >> that start with a minus sign
are shifted from the array, i.e. they are not considered as column
names, but are re-injected later into the SQL (without the minus sign), 
just after the C<SELECT> keyword. This is especially useful for 

  $sqla->select(..., -columns => [-DISTINCT => @columns], ...);

However, it may also be useful for other purposes, like
vendor-specific SQL variants :

   # MySQL features
  ->select(..., -columns => [-STRAIGHT_JOIN    => @columns], ...);
  ->select(..., -columns => [-SQL_SMALL_RESULT => @columns], ...);

   # Oracle hint
  ->select(..., -columns => ["-/*+ FIRST_ROWS (100) */" => @columns], ...);

The argument to C<-columns> can also be a string instead of 
an arrayref, like for example
C<< "c1 AS foobar, MAX(c2) AS m_c2, COUNT(c3) AS n_c3" >>;
however this is mainly for backwards compatibility. The 
recommended way is to use the arrayref notation as explained above :

  -columns => [ qw/  c1|foobar   MAX(c2)|m_c2   COUNT(c3)|n_c3  / ]

If omitted, C<< -columns >> takes '*' as default argument.

=item C<< -from => $table || \@joined_tables >> 


=item C<< -where => \%where >>

C<< \%where >> is a reference to a hash or array of 
criteria that will be translated into SQL clauses. In most cases, this
will just be something like C<< {col1 => 'val1', col2 => 'val2'} >>;
see L<SQL::Abstract::select|SQL::Abstract/select> for 
 detailed description of the
structure of that hash or array. It can also be
a plain SQL string like C<< "col1 IN (3, 5, 7, 11) OR col2 IS NOT NULL" >>.

=item C<< -group_by => "string" >>  or C<< -group_by => \@array >> 

adds a C<GROUP BY> clause in the SQL statement. Grouping columns are
specified either by a plain string or by an array of strings.

=item C<< -having => "string" >>  or C<< -having => \%criteria >> 

adds a C<HAVING> clause in the SQL statement (only makes
sense together with a C<GROUP BY> clause).
This is like a C<-where> clause, except that the criteria
are applied after grouping has occured.


=item C<< -order_by => \@order >>

C<< \@order >> is a reference to a list 
of columns for sorting. Columns can 
be prefixed by '+' or '-' for indicating sorting directions,
so for example C<< -orderBy => [qw/-col1 +col2 -col3/] >>
will generate the SQL clause
C<< ORDER BY col1 DESC, col2 ASC, col3 DESC >>.
Alternatively, columns can be specified
as hashrefs in the form C<< {-asc => $column_name} >>
or C<< {-desc => $column_name} >>.

The whole C<< -order_by >> parameter can also be a plain SQL string
like C<< "col1 DESC, col3, col2 DESC" >>.


=item C<< -page_size => $page_size >>

specifies how many rows will be retrieved per "page" of data.
Default is unlimited (or more precisely the maximum 
value of a short integer on your system).
When specified, this parameter automatically implies C<< -limit >>.

=item C<< -page_index => $page_index >>

specifies the page number (starting at 1). Default is 1.
When specified, this parameter automatically implies C<< -offset >>.

=item C<< -limit => $limit >>

limit to the number of rows that will be retrieved.
Automatically implied by C<< -page_size >>.

=item C<< -offset => $offset >>

Automatically implied by C<< -page_index >>.
Defaults to 0.

=item C<< -for => $clause >> 

specifies an additional clause to be added at the end of the SQL statement,
like C<< -for => 'READ ONLY' >> or C<< -for => 'UPDATE' >>.

=item C<< -want_details => 1 >>

If true, the return value will be a hashref instead of the usual
C<< ($sql, @bind) >>. The hashref contains the following keys :

=over

=item sql

generated SQL

=item bind

bind values

=item aliased_tables

a hashref of  C<< {table_alias => table_name} >> encountered while
parsing the C<-from> parameter.

=item aliased_columns

a hashref of  C<< {column_alias => column_name} >> encountered while
parsing the C<-columns> parameter.

=back


=back



=head2 insert

  # positional parameters, directly passed to the parent class
  ($sql, @bind) = $sqla->insert($table, \@values || \%fieldvals, \%options);

  # named parameters, handled in this class 
  ($sql, @bind) = $sqla->insert(
    -into      => $table,
    -values    => {col => $val, ...},
    -returning => $return_structure,
  );

Named parameters to the C<insert()> method are just syntactic sugar
for better readability of the client's code; they are passed verbatim
to the parent method. Parameter C<-returning> is optional and only
supported by some database vendors; see L<SQL::Abstract/insert>.

=head2 update

  # positional parameters, directly passed to the parent class
  ($sql, @bind) = $sqla->update($table, \%fieldvals, \%where);

  # named parameters, handled in this class 
  ($sql, @bind) = $sqla->update(
    -table => $table,
    -set   => {col => $val, ...},
    -where => \%conditions,
  );

Named parameters to the C<update()> method are just syntactic sugar
for better readability of the client's code; they are passed verbatim
to the parent method. 


=head2 delete

  # positional parameters, directly passed to the parent class
  ($sql, @bind) = $sqla->delete($table, \%where);

  # named parameters, handled in this class 
  ($sql, @bind) = $sqla->delete (
    -from  => $table
    -where => \%conditions,
  );

Named parameters to the C<delete()> method are just syntactic sugar
for better readability of the client's code; they are passed verbatim
to the parent method.


=head2 table_alias

  my $sql = $sqla->table_alias($table_name, $alias);

Returns the SQL fragment for aliasing a table.
If C<$alias> is empty, just returns C<$table_name>.

=head2 column_alias

Like C<table_alias>, but for column aliasing.

=head2 limit_offset

  ($sql, @bind) = $sqla->limit_offset($limit, $offset);

Generates C<($sql, @bind)> for a LIMIT-OFFSET clause.

=head2 join

  ($sql, @bind) = $sqla->join(
    <table0> <join_1> <table_1> ... <join_n> <table_n>
  );

Generates C<($sql, @bind)> for a JOIN clause, taking as input
a collection of joined tables with their join conditions.
The following example gives an idea of the available syntax :

  ($sql, @bind) = $sqla->join(qw[
     Table1|t1       ab=cd         Table2|t2
                 <=>{ef>gh,ij<kl}  Table3
                  =>{t1.mn=op}     Table4
     ]);

This will generate

  Table1 AS t1 INNER JOIN Table2 AS t2 ON t1.ab=t2.cd
               INNER JOIN Table3       ON t2.ef>Table3.gh 
                                      AND t2.ij<Table3.kl
                LEFT JOIN Table4       ON t1.mn=Table4.op

More precisely, the arguments to C<join()> should be a list
containing an odd number of elements, where the odd positions
are I<table specifications> and the even positions are
I<join specifications>. 

=head3 Table specifications

A table specification for join is a string containing
the table name, possibly followed by a vertical bar
and an alias name. For example C<Table1> or C<Table1|t1>
are valid table specifications.

These are converted into internal hashrefs with keys
C<sql>, C<bind>, C<name>, C<aliased_tables>, like this :

  {
    sql            => "Table1 AS t1"
    bind           => [],
    name           => "t1"
    aliased_tables => {"t1" => "Table1"}
  }

Such hashrefs can be passed directly as arguments,
instead of the simple string representation.

=head3 Join specifications

A join specification is a string containing
an optional I<join operator>, possibly followed
by a pair of curly braces or square brackets
containing the I<join conditions>. 

Default builtin join operators are
C<< <=> >>, C<< => >>, C<< <= >>, C<< == >>,
corresponding to the following
SQL JOIN clauses :

  '<=>' => '%s INNER JOIN %s ON %s',
   '=>' => '%s LEFT OUTER JOIN %s ON %s',
  '<='  => '%s RIGHT JOIN %s ON %s',
  '=='  => '%s NATURAL JOIN %s',

This operator table can be overridden through
the C<join_syntax> parameter of the L</new> method.

The join conditions is a comma-separated list
of binary column comparisons, like for example

  {ab=cd,Table1.ef<Table2.gh}

Table names may be explicitly given using dot notation,
or may be implicit, in which case they will be filled
automatically from the names of operands on the
left-hand side and right-hand side of the join.

In accordance with L<SQL::Abstract> common conventions, 
if the list of comparisons is within curly braces, it will
become an C<AND>; if it is within square brackets, it will
become an C<OR>.

Join specifications expressed as strings
are converted into internal hashrefs with keys
C<operator> and  C<condition>, like this :

  {
    operator  => '<=>',
    condition => { '%1$s.ab' => \'= %2$s.cd',
                   '%1$s.ef' => \'= Table2.gh'}
  }

The C<operator> is a key into the C<join_syntax> table; the associated
value is a sprinf format string, with placeholders for the left and
right operands, and the join condition.  The C<condition> is a
structure suitable for being passed as argument to
L<SQL::Abstract/where>.  Places where the names of left/right tables
(or their aliases) are expected should be expressed as sprintf
placeholders, i.e.  respectively C<%1$s> and C<%2$s>.  Beware that the
right-hand side of the condition should most likely B<not> belong to
the C<@bind> list, so in order to prevent that you need to prepend a
backslash in front of strings on the right-hand side ... but then you
also need to supply the '=' comparison operator.

Hashrefs for join specifications 
can be passed directly as arguments,
instead of the simple string representation.

=head2 merge_conditions 

  my $conditions = $sqla->merge_conditions($cond_A, $cond_B, ...);

This utility method takes a list of "C<where>" conditions and
merges all of them in a single hashref. For example merging

  ( {a => 12, b => {">" => 34}}, 
    {b => {"<" => 56}, c => 78} )

produces 

  {a => 12, b => [-and => {">" => 34}, {"<" => 56}], c => 78});

=head1 TODO

Future versions may include some of these features :

=over

=item *

maybe named parameters for insert/update/delete. These would not 
be extremely useful; but for the sake of consistency it's probably
worth implementing.

=item *

support for C<WITH> initial clauses, and C<WITH RECURSIVE>.

=item *

suport for Oracle-specific syntax for recursive queries
(START_WITH, PRIOR, CONNECT_BY NOCYCLE, CONNECT SIBLINGS, etc.)

=item *

support for INSERT variants

    INSERT .. DEFAULT VALUES
    INSERT .. VALUES(), VALUES()
    INSERT .. RETURNING

=item *

support for MySQL C<LOCK_IN_SHARE_MODE>

=item * 

support for UNION, INTERSECT, EXCEPT|MINUS, etc. The syntax
will probably be something like

   select(-columns => ..
          -from    => ...
          -union => [-columns =>
                     -from   => 
                     -intersect => [
                         ..
                       ]
   # beware : ORDER_BY / LIMIT come after the union

=item *

new constructor option 

  ->new(..., select_implicitly_for => $string, ...)

This would provide a default values for the C<-for> parameter.

=back

=head1 AUTHOR

Laurent Dami, C<< <laurent.dami at justice.ge.ch> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-sql-abstract-more at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=SQL-Abstract-More>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc SQL::Abstract::More


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=SQL-Abstract-More>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/SQL-Abstract-More>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/SQL-Abstract-More>

=item * Search CPAN

L<http://search.cpan.org/dist/SQL-Abstract-More/>

=back


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Laurent Dami.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut


