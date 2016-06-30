package SQL::Abstract::More;
use strict;
use warnings;

use SQL::Abstract 1.73;
use parent 'SQL::Abstract';
use MRO::Compat;
use mro 'c3'; # implements next::method

use Params::Validate  qw/validate SCALAR SCALARREF CODEREF ARRAYREF HASHREF
                                  UNDEF  BOOLEAN/;
use Scalar::Util      qw/blessed reftype/;
use Carp;

our $VERSION = '1.27';

# import the "puke" function from SQL::Abstract (kind of "die")
BEGIN {*puke = \&SQL::Abstract::puke;}


#----------------------------------------------------------------------
# utility function : cheap version of Scalar::Does (too heavy to be included)
#----------------------------------------------------------------------
my %meth_for = (
  ARRAY => '@{}',
  HASH  => '%{}',
 );

sub does ($$) {
  my ($data, $type) = @_;
  my $reft = reftype $data;
  return defined $reft && $reft eq $type
      || blessed $data && overload::Method($data, $meth_for{$type});
}





#----------------------------------------------------------------------
# remove all previously defined functions
#----------------------------------------------------------------------
use namespace::clean;


#----------------------------------------------------------------------
# global variables
#----------------------------------------------------------------------

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
    # See also L<DBIx::DataModel> : within that ORM an additional layer is
    # added to take advantage of Oracle scrollable cursors.
    my $sql = "SELECT * FROM ("
            .   "SELECT subq_A.*, ROWNUM rownum__index FROM (%s) subq_A "
            .   "WHERE ROWNUM <= ?"
            .  ") subq_B WHERE rownum__index >= ?";

    no warnings 'uninitialized'; # in case $limit or $offset is undef
    # row numbers start at 1
    return $sql, $offset + $limit, $offset + 1;
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
  table_alias          => {type => SCALAR|CODEREF, default  => '%s AS %s'},
  column_alias         => {type => SCALAR|CODEREF, default  => '%s AS %s'},
  limit_offset         => {type => SCALAR|CODEREF, default  => 'LimitOffset'},
  join_syntax          => {type => HASHREF,        default  =>
                                                        \%common_join_syntax},
  join_assoc_right     => {type => BOOLEAN,        default  => 0},
  max_members_IN       => {type => SCALAR,         optional => 1},
  multicols_sep        => {type => SCALAR|SCALARREF, optional => 1},
  has_multicols_in_SQL => {type => BOOLEAN,        optional => 1},
  sql_dialect          => {type => SCALAR,         optional => 1},
);

# builtin collection of parameters, for various databases
my %sql_dialects = (
 MsAccess  => { join_assoc_right     => 1,
                join_syntax          => \%right_assoc_join_syntax},
 BasisJDBC => { column_alias         => "%s %s",
                max_members_IN       => 255                      },
 MySQL_old => { limit_offset         => "LimitXY"                },
 Oracle    => { limit_offset         => "RowNum",
                max_members_IN       => 999,
                table_alias          => '%s %s',
                column_alias         => '%s %s',
                has_multicols_in_SQL => 1,                       },
);


# operators for compound queries
my @set_operators = qw/union union_all intersect minus except/;

# specification of parameters accepted by select, insert, update, delete
my %params_for_select = (
  -columns      => {type => SCALAR|ARRAYREF,         default  => '*'},
  -from         => {type => SCALAR|SCALARREF|ARRAYREF},
  -where        => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  (map {-$_ => {type => ARRAYREF, optional => 1}} @set_operators),
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
  -returning    => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
);
my %params_for_update = (
  -table        => {type => SCALAR},
  -set          => {type => HASHREF},
  -where        => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  -order_by     => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  -limit        => {type => SCALAR,                  optional => 1},
);
my %params_for_delete = (
  -from         => {type => SCALAR},
  -where        => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  -order_by     => {type => SCALAR|ARRAYREF|HASHREF, optional => 1},
  -limit        => {type => SCALAR,                  optional => 1},
);


#----------------------------------------------------------------------
# object creation
#----------------------------------------------------------------------

sub new {
  my $class = shift;
  my %params = does($_[0], 'HASH') ? %{$_[0]} : @_;

  # extract params for this subclass
  my %more_params;
  foreach my $key (keys %params_for_new) {
    $more_params{$key} = delete $params{$key} if exists $params{$key};
  }

  # import params from SQL dialect, if any
  my $dialect = delete $more_params{sql_dialect};
  if ($dialect) {
    my $dialect_params = $sql_dialects{$dialect}
      or croak "no such sql dialect: $dialect";
    $more_params{$_} ||= $dialect_params->{$_} foreach keys %$dialect_params;
  }

  # check parameters
  my @more_params = %more_params;
  my $more_self   = validate(@more_params, \%params_for_new);

  # call parent constructor
  my $self = $class->next::method(%params);
    # TODO - validate %params -- because SQLA doesn't do it :-(

  # inject into $self
  $self->{$_} = $more_self->{$_} foreach keys %$more_self;

  # arguments supplied as scalars are transformed into coderefs
  ref $self->{column_alias} or $self->_make_AS_through_sprintf('column_alias');
  ref $self->{table_alias}  or $self->_make_AS_through_sprintf('table_alias');
  ref $self->{limit_offset} or $self->_choose_LIMIT_OFFSET_dialect;

  # regex for parsing join specifications
  my @join_ops = sort {length($b) <=> length($a) || $a cmp $b}
                      keys %{$self->{join_syntax}};
  my $joined_ops = join '|', map quotemeta, @join_ops;
  $self->{join_regex} = qr[
     ^              # initial anchor 
     ($joined_ops)? # $1: join operator (i.e. '<=>', '=>', etc.))
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
    # extract alias, if any
    if ($col =~ /^\s*         # ignore insignificant leading spaces
                 (.*[^|\s])   # any non-empty string, not ending with ' ' or '|'
                 \|           # followed by a literal '|'
                 (\w+)        # followed by a word (the alias))
                 \s*          # ignore insignificant trailing spaces
                 $/x) {
      $aliased_columns{$2} = $1;
      $col = $self->column_alias($1, $2);
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

  # generate initial ($sql, @bind), without -order_by (will be handled later)
  my @old_API_args = @args{qw/-from -columns -where/}; #
  my ($sql, @bind) = $self->next::method(@old_API_args);
  unshift @bind, @{$join_info->{bind}} if $join_info;

  # add @post_select clauses if needed (for ex. -distinct)
  my $post_select = join " ", @post_select;
  $sql =~ s[^SELECT ][SELECT $post_select ]i if $post_select;

  # add set operators (UNION, INTERSECT, etc) if needed
  foreach my $set_op (@set_operators) {
    if ($args{-$set_op}) {
      my %sub_args = @{$args{-$set_op}};
      $sub_args{$_} ||= $args{$_} for qw/-columns -from/;
      my ($sql1, @bind1) = $self->select(%sub_args);
      (my $sql_op = uc($set_op)) =~ s/_/ /g;
      $sql .= " $sql_op $sql1";
      push @bind, @bind1;
    }
  }

  # add GROUP BY/HAVING if needed
  if ($args{-group_by}) {
    my $sql_grp = $self->where(undef, $args{-group_by});
    $sql_grp =~ s/\bORDER\b/GROUP/;
    if ($args{-having}) {
      my ($sql_having, @bind_having) = $self->where($args{-having});
      $sql_having =~ s/\bWHERE\b/HAVING/;
      $sql_grp .= " $sql_having";
      push @bind, @bind_having;
    }
    $sql .= $sql_grp;
  }

  # add ORDER BY if needed
  if (my $order = $args{-order_by}) {

    # force scalar into an arrayref
    $order = [$order] if not ref $order;

    # restructure array data
    if (ref $order eq 'ARRAY') {
      my @clone = @$order;      # because we will modify items 

      # '-' and '+' prefixes are translated into {-desc/asc => } hashrefs
      foreach my $item (@clone) {
        next if !$item or ref $item;
        $item =~ s/^-//  and $item = {-desc => $item} and next;
        $item =~ s/^\+// and $item = {-asc  => $item};
      }
      $order = \@clone;
    }

    my $sql_order = $self->where(undef, $order);
    $sql .= $sql_order;
  }

  # add LIMIT/OFFSET if needed
  if (defined $args{-limit}) {
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
  my $returning_into;

  if (&_called_with_named_args) {
    # extract named args and translate to old SQLA API
    my %args = validate(@_, \%params_for_insert);
    @old_API_args = @args{qw/-into -values/};

    # if present, "-returning" may be a scalar, arrayref or hashref; the latter
    # is interpreted as .. RETURNING ... INTO ...
    if (my $returning = $args{-returning}) {
      if (does $returning, 'HASH') {
        my @keys = sort keys %$returning
          or croak "-returning => {} : the hash is empty";
        push @old_API_args, {returning => \@keys};
        $returning_into = [@{$returning}{@keys}];
      }
      else {
        push @old_API_args, {returning => $returning};
      }
    }
  }
  else {
    @old_API_args = @_;
  }

  # get results from parent method
  my ($sql, @bind) = $self->next::method(@old_API_args);

  # inject more stuff if using Oracle's "RETURNING ... INTO ..."
  if ($returning_into) {
    $sql .= ' INTO ' . join(", ", ("?") x @$returning_into);
    push @bind, @$returning_into;
  }

  return ($sql, @bind);
}

sub update {
  my $self = shift;

  my @old_API_args;
  my %args;
  if (&_called_with_named_args) {
    %args = validate(@_, \%params_for_update);
    @old_API_args = @args{qw/-table -set -where/};
  }
  else {
    @old_API_args = @_;
  }

  # call clone of parent method
  my ($sql, @bind) = $self->_overridden_update(@old_API_args);

  # maybe need to handle additional args
  $self->_handle_additional_args_for_update_delete(\%args, \$sql, \@bind);

  return ($sql, @bind);
}


sub _handle_additional_args_for_update_delete {
  my ($self, $args, $sql_ref, $bind_ref) = @_;

  if (defined $args->{-order_by}) {
    my ($sql_ob, @bind_ob) = $self->_order_by($args->{-order_by});
    $$sql_ref .= $sql_ob;
    push @$bind_ref, @bind_ob;
  }
  if (defined $args->{-limit}) {
    # can't call $self->limit_offset(..) because there shouldn't be any offset
    $$sql_ref .= $self->_sqlcase(' limit ?');
    push @$bind_ref, $args->{-limit};
  }
}



sub delete {
  my $self = shift;

  my @old_API_args;
  my %args;
  if (&_called_with_named_args) {
    %args = validate(@_, \%params_for_delete);
    @old_API_args = @args{qw/-from -where/};
  }
  else {
    @old_API_args = @_;
  }

  # call parent method
  my ($sql, @bind) = $self->next::method(@old_API_args);

  # maybe need to handle additional args
  $self->_handle_additional_args_for_update_delete(\%args, \$sql, \@bind);

  return ($sql, @bind);
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
    my $table_spec = shift or croak "join(): improper number of operands";

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
    if    (does $cond, 'HASH')  {
      foreach my $col (sort keys %$cond) {
        $merged{$col} = $merged{$col} ? [-and => $merged{$col}, $cond->{$col}]
                                      : $cond->{$col};
      }
    }
    elsif (does $cond, 'ARRAY') {
      $merged{-nest} = $merged{-nest} ? {-and => [$merged{-nest}, $cond]}
                                      : $cond;
    }
    elsif ($cond) {
      $merged{$cond} = \"";
    }
  }
  return \%merged;
}

# utility for calling either bind_param or bind_param_inout
our $INOUT_MAX_LEN = 99; # chosen arbitrarily; see L<DBI/bind_param_inout>
sub bind_params {
  my ($self, $sth, @bind) = @_;
  $sth->isa('DBI::st') or croak "sth argument is not a DBI statement handle";
  foreach my $i (0 .. $#bind) {
    my $val = $bind[$i];
    my $ref = ref $val || '';
    if ($ref eq 'SCALAR') {
      # a scalarref is interpreted as an INOUT parameter
      $sth->bind_param_inout($i+1, $val, $INOUT_MAX_LEN);
    }
    elsif ($ref eq 'ARRAY' and
             my ($bind_meth, @args) = $self->is_bind_value_with_type($val)) {
      # either 'bind_param' or 'bind_param_inout', with 2 or 3 args
      $sth->$bind_meth($i+1, @args);
    }
    else {
      # other cases are passed directly to DBI::bind_param
      $sth->bind_param($i+1, $val);
    }
  }
}

sub is_bind_value_with_type {
  my ($self, $val) = @_;

  # compatibility with DBIx::Class syntax of shape [\%args => $val],
  # see L<DBIx::Class::ResultSet/"DBIC BIND VALUES">
  if (   @$val == 2
      && does($val->[0], 'HASH')
      && grep {$val->[0]{$_}} qw/dbd_attrs sqlt_size
                                 sqlt_datatype dbic_colname/) {
    my $args = $val->[0];
    if (my $attrs = $args->{dbd_attrs}) {
      return (bind_param => $val->[1], $attrs);
    }
    elsif (my $size = $args->{sqlt_size}) {
      return (bind_param_inout => $val, $size);
    }
    # other options like 'sqlt_datatype', 'dbic_colname' are not supported
    else {
      croak "unsupported options for bind type : "
           . CORE::join(", ", sort keys %$args);
    }

    # NOTE : the following DBIx::Class shortcuts are not supported
    #  [ $name => $val ] === [ { dbic_colname => $name }, $val ]
    #  [ \$dt  => $val ] === [ { sqlt_datatype => $dt }, $val ]
    #  [ undef,   $val ] === [ {}, $val ]
  }

  # in all other cases, this is not a bind value with type
  return ();
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
    or croak "empty join specification";
  my ($op, $bracket, $cond_list) = ($join_spec =~ $self->{join_regex})
    or croak "incorrect join specification : $join_spec\n$self->{join_regex}";
  $op        ||= '<=>';
  $bracket   ||= '{';
  $cond_list ||= '';

  # extract constants (strings between quotes), replaced by placeholders
  my $regex = qr/'       # initial quote
                 (       # begin capturing group
                  [^']*    # any non-quote chars
                  (?:        # begin non-capturing group
                     ''        # pair of quotes
                     [^']*     # any non-quote chars
                  )*         # this non-capturing group 0 or more times
                 )       # end of capturing group
                 '       # ending quote
                /x;
  my $placeholder = '_?_'; # unlikely to be counfounded with any value 
  my @constants;
  while ($cond_list =~ s/$regex/$placeholder/) {
    push @constants, $1;
  };
  s/''/'/g for @constants;  # replace pairs of quotes by single quotes

  # accumulate conditions as pairs ($left => \"$op $right")
  my @conditions;
  foreach my $cond (split /,/, $cond_list) {
    # parse the condition (left and right operands + comparison operator)
    my ($left, $cmp, $right) = split /([<>=!^]{1,2})/, $cond
      or croak "can't parse join condition: $cond";

    # if operands are not qualified by table/alias name, add sprintf hooks
    $left  = "%1\$s.$left"  unless $left  =~ /\./;
    $right = "%2\$s.$right" unless $right =~ /\./ or $right eq $placeholder;

    # add this pair into the list; right operand is either a bind value
    # or an identifier within the right table
    $right = $right eq $placeholder ? shift @constants : {-ident => $right};
    push @conditions, $left, {$cmp => $right};
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
  {
    no warnings 'redundant';
    $sql = sprintf $sql, $left->{name}, $right->{name};
  }

  # assemble all elements
  my $syntax = $self->{join_syntax}{$join_spec->{operator}};
  $sql = sprintf $syntax, $left->{sql}, $right->{sql}, $sql;
  unshift @bind, @{$left->{bind}}, @{$right->{bind}};

  # build result and return
  my %result = (sql => $sql, bind => \@bind);
  $result{name} = ($self->{join_assoc_right} ? $left : $right)->{name};
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

  # special algorithm if the key is multi-columns (contains a multicols_sep)
  if ($self->{multicols_sep}) {
    my @cols = split m[$self->{multicols_sep}], $k;
    if (@cols > 1) {
      if ($self->{has_multicols_in_SQL}) {
        # DBMS accepts special SQL syntax for multicolumns
        return $self->_multicols_IN_through_SQL(\@cols, $op, $vals);
      }
      else {
        # DBMS doesn't accept special syntax, so we must use boolean logic
        return $self->_multicols_IN_through_boolean(\@cols, $op, $vals);
      }
    }
  }

  # special algorithm if the number of values exceeds the allowed maximum
  my $max_members_IN = $self->{max_members_IN};
  if ($max_members_IN && does($vals, 'ARRAY')
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


  # otherwise, call parent method
  $vals = [@$vals] if blessed $vals; # because SQLA dies on blessed arrayrefs
  return $self->next::method($k, $op, $vals);
}


sub _multicols_IN_through_SQL {
  my ($self, $cols, $op, $vals) = @_;

  # build initial sql
  my $n_cols   = @$cols;
  my $sql_cols = CORE::join(',', map {$self->_quote($_)} @$cols);
  my $sql      = "($sql_cols) " . $self->_sqlcase($op);

  # dispatch according to structure of $vals
  return $self->_SWITCH_refkind($vals, {

    ARRAYREF => sub {    # list of tuples
      # deal with special case of empty list (like the parent class)
      my $n_tuples = @$vals;
      if (!$n_tuples) {
        my $sql = ($op =~ /\bnot\b/i) ? $self->{sqltrue} : $self->{sqlfalse};
        return ($sql);
      }

      # otherwise, build SQL and bind values for the list of tuples
      my @bind;
      foreach my $val (@$vals) {
        does($val, 'ARRAY')
          or $val = [split  m[$self->{multicols_sep}], $val];
        @$val == $n_cols
          or puke "op '$op' with multicols: tuple with improper number of cols";
        push @bind, @$val;
      }
      my $single_tuple = "(" . CORE::join(',', (('?') x $n_cols)) . ")";

      my $all_tuples   = CORE::join(', ', (($single_tuple) x $n_tuples));
      $sql            .= " ($all_tuples)";
      return ($sql, @bind);
    },

    SCALARREF => sub {   # literal SQL
      $sql .= " ($$vals)";
      return ($sql);
    },

    ARRAYREFREF => sub { # literal SQL with bind
      my ($inner_sql, @bind) = @$$vals;
      $sql .= " ($inner_sql)";
      return ($sql, @bind);
    },

    FALLBACK => sub {
      puke "op '$op' with multicols requires a list of tuples or literal SQL";
    },

   });
}


sub _multicols_IN_through_boolean {
  my ($self, $cols, $op, $vals) = @_;

  # can't handle anything else than a list of tuples
  does($vals, 'ARRAY') && @$vals
    or puke "op '$op' with multicols requires a non-empty list of tuples";

  # assemble SQL
  my $n_cols   = @$cols;
  my $sql_cols = CORE::join(' AND ', map {$self->_quote($_) . " = ?"} @$cols);
  my $sql      = "(" . CORE::join(' OR ', (("($sql_cols)") x @$vals)) . ")";
  $sql         = "NOT $sql" if $op =~ /\bnot\b/i;

  # assemble bind values
  my @bind;
  foreach my $val (@$vals) {
    does($val, 'ARRAY')
      or $val = [split  m[$self->{multicols_sep}], $val];
    @$val == $n_cols
      or puke "op '$op' with multicols: tuple with improper number of cols";
    push @bind, @$val;
  }

  # return the whole thing
  return ($sql, @bind);
}



#----------------------------------------------------------------------
# override of parent's methods for decoding arrayrefs
#----------------------------------------------------------------------

sub _where_hashpair_ARRAYREF {
  my ($self, $k, $v) = @_;

  if ($self->is_bind_value_with_type($v)) {
    $self->_assert_no_bindtype_columns;
    my $sql = CORE::join ' ', $self->_convert($self->_quote($k)),
                              $self->_sqlcase($self->{cmp}),
                              $self->_convert('?');
    my @bind = ($v);
    return ($sql, @bind);
  }
  else {
    return $self->next::method($k, $v);
  }
}


sub _where_field_op_ARRAYREF {
  my ($self, $k, $op, $vals) = @_;

  if ($self->is_bind_value_with_type($vals)) {
    $self->_assert_no_bindtype_columns;
    my $sql = CORE::join ' ', $self->_convert($self->_quote($k)),
                              $self->_sqlcase($op),
                              $self->_convert('?');
    my @bind = ($vals);
    return ($sql, @bind);
  }
  else {
    return $self->next::method($k, $op, $vals);
  }
}

sub _assert_no_bindtype_columns {
  my ($self) = @_;
  $self->{bindtype} ne 'columns'
    or croak 'values of shape [$val, \%type] are not compatible'
           . 'with ...->new(bindtype => "columns")';
}

sub _insert_values {
  # unfortunately, we can't just override the ARRAYREF part, so the whole
  # parent method is copied here
  my ($self, $data) = @_;

  my (@values, @all_bind);
  foreach my $column (sort keys %$data) {
    my $v = $data->{$column};

    $self->_SWITCH_refkind($v, {

      ARRAYREF => sub {
        if ($self->{array_datatypes}
            || $self->is_bind_value_with_type($v)) { 
          # if array datatype are activated or this is a [$val, \%type] struct
          push @values, '?';
          push @all_bind, $self->_bindtype($column, $v);
        }
        else {
          # otherwise, literal SQL with bind
          my ($sql, @bind) = @$v;
          $self->_assert_bindval_matches_bindtype(@bind);
          push @values, $sql;
          push @all_bind, @bind;
        }
      },

      ARRAYREFREF => sub { # literal SQL with bind
        my ($sql, @bind) = @${$v};
        $self->_assert_bindval_matches_bindtype(@bind);
        push @values, $sql;
        push @all_bind, @bind;
      },

      # THINK : anything useful to do with a HASHREF ?
      HASHREF => sub {  # (nothing, but old SQLA passed it through)
        #TODO in SQLA >= 2.0 it will die instead
        SQL::Abstract::belch("HASH ref as bind value in insert is not supported");
        push @values, '?';
        push @all_bind, $self->_bindtype($column, $v);
      },

      SCALARREF => sub {  # literal SQL without bind
        push @values, $$v;
      },

      SCALAR_or_UNDEF => sub {
        push @values, '?';
        push @all_bind, $self->_bindtype($column, $v);
      },

     });

  }

  my $sql = $self->_sqlcase('values')." ( ".CORE::join(", ", @values)." )";
  return ($sql, @all_bind);
}

sub _overridden_update {
  # unfortunately, we can't just override the ARRAYREF part, so the whole
  # parent method is copied here

  my $self  = shift;
  my $table = $self->_table(shift);
  my $data  = shift || return;
  my $where = shift;

  # first build the 'SET' part of the sql statement
  my (@set, @all_bind);
  puke "Unsupported data type specified to \$sql->update"
    unless ref $data eq 'HASH';

  for my $k (sort keys %$data) {
    my $v = $data->{$k};
    my $r = ref $v;
    my $label = $self->_quote($k);

    $self->_SWITCH_refkind($v, {
      ARRAYREF => sub {
        if ($self->{array_datatypes}
            || $self->is_bind_value_with_type($v)) {
          push @set, "$label = ?";
          push @all_bind, $self->_bindtype($k, $v);
        }
        else {                          # literal SQL with bind
          my ($sql, @bind) = @$v;
          $self->_assert_bindval_matches_bindtype(@bind);
          push @set, "$label = $sql";
          push @all_bind, @bind;
        }
      },
      ARRAYREFREF => sub { # literal SQL with bind
        my ($sql, @bind) = @${$v};
        $self->_assert_bindval_matches_bindtype(@bind);
        push @set, "$label = $sql";
        push @all_bind, @bind;
      },
      SCALARREF => sub {  # literal SQL without bind
        push @set, "$label = $$v";
      },
      HASHREF => sub {
        my ($op, $arg, @rest) = %$v;

        puke 'Operator calls in update must be in the form { -op => $arg }'
          if (@rest or not $op =~ /^\-(.+)/);

        local $self->{_nested_func_lhs} = $k;
        my ($sql, @bind) = $self->_where_unary_op ($1, $arg);

        push @set, "$label = $sql";
        push @all_bind, @bind;
      },
      SCALAR_or_UNDEF => sub {
        push @set, "$label = ?";
        push @all_bind, $self->_bindtype($k, $v);
      },
    });
  }

  # generate sql
  my $sql = $self->_sqlcase('update') . " $table " . $self->_sqlcase('set ')
          . CORE::join ', ', @set;

  if ($where) {
    my($where_sql, @where_bind) = $self->where($where);
    $sql .= $where_sql;
    push @all_bind, @where_bind;
  }

  return wantarray ? ($sql, @all_bind) : $sql;
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
    or croak "no such limit_offset dialect: $dialect";
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

Generates SQL from Perl data structures.  This is a subclass of
L<SQL::Abstract>, fully compatible with the parent class, but with
some improvements :

=over

=item *

methods take arguments as I<named parameters> instead of positional parameters.
This is more flexible for identifying and assembling various SQL clauses,
like C<-where>, C<-order_by>, C<-group_by>, etc.

=item *

additional SQL constructs like C<-union>, C<-group_by>, C<join>, etc.
are supported

=item *

C<WHERE .. IN> clauses can range over multiple columns (tuples)

=item *

values passed to C<select>, C<insert> or C<update> can directly incorporate
information about datatypes, in the form of arrayrefs of shape
C<< [{dbd_attrs => \%type}, $value] >>

=item *

several I<SQL dialects> can adapt the generated SQL to various DBMS vendors

=back

This module was designed for the specific needs of
L<DBIx::DataModel>, but is published as a standalone distribution,
because it may possibly be useful for other needs.

Unfortunately, this module cannot be used with L<DBIx::Class>, because
C<DBIx::Class> creates its own instance of C<SQL::Abstract>
and has no API to let the client instantiate from any other class.

=head1 SYNOPSIS

  my $sqla = SQL::Abstract::More->new();
  my ($sql, @bind);

  # ex1: named parameters, select DISTINCT, ORDER BY, LIMIT/OFFSET
  ($sql, @bind) = $sqla->select(
   -columns  => [-distinct => qw/col1 col2/],
   -from     => 'Foo',
   -where    => {bar => {">" => 123}},
   -order_by => [qw/col1 -col2 +col3/],  # BY col1, col2 DESC, col3 ASC
   -limit    => 100,
   -offset   => 300,
  );

  # ex2: column aliasing, join
  ($sql, @bind) = $sqla->select(
    -columns => [         qw/Foo.col_A|a           Bar.col_B|b /],
    -from    => [-join => qw/Foo           fk=pk   Bar         /],
  );

  # ex3: INTERSECT (or similar syntax for UNION)
  ($sql, @bind) = $sqla->select(
    -columns => [qw/col1 col2/],
    -from    => 'Foo',
    -where   => {col1 => 123},
    -intersect => [ -columns => [qw/col3 col4/],
                    -from    => 'Bar',
                    -where   => {col3 => 456},
                   ],
  );

  # ex4: passing datatype specifications
  ($sql, @bind) = $sqla->select(
   -from     => 'Foo',
   -where    => {bar => [{dbd_attrs => {ora_type => ORA_XMLTYPE}}, $xml]},
  );
  my $sth = $dbh->prepare($sql);
  $sqla->bind_params($sth, @bind);
  $sth->execute;

  # ex5: multicolumns-in
  $sqla = SQL::Abstract::More->new(
    multicols_sep        => '/',
    has_multicols_in_SQL => 1,
  );
  ($sql, @bind) = $sqla->select(
   -from     => 'Foo',
   -where    => {"foo/bar/buz" => {-in => ['1/a/X', '2/b/Y', '3/c/Z']}},
  );

  # merging several criteria
  my $merged = $sqla->merge_conditions($cond_A, $cond_B, ...);
  ($sql, @bind) = $sqla->select(..., -where => $merged, ..);

  # insert / update / delete
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

A C<sprintf> format description for generating table aliasing clauses.
The default is C<%s AS %s>.
Can also be supplied as a method coderef (see L</"Overriding methods">).

=item column_alias

A C<sprintf> format description for generating column aliasing clauses.
The default is C<%s AS %s>.
Can also be supplied as a method coderef.

=item limit_offset

Name of a "limit-offset dialect", which can be one of
C<LimitOffset>, C<LimitXY>, C<LimitYX> or C<RowNum>; 
see L<SQL::Abstract::Limit> for an explanation of those dialects.
Here, unlike the L<SQL::Abstract::Limit> implementation,
limit and offset values are treated as regular values,
with placeholders '?' in the SQL; values are postponed to the
C<@bind> list.

The argument can also be a coderef (see below
L</"Overriding methods">). That coderef takes C<$self, $limit, $offset>
as arguments, and should return C<($sql, @bind)>. If C<$sql> contains
C<%s>, it is treated as a C<sprintf> format string, where the original
SQL is injected into C<%s>.


=item join_syntax

A hashref where keys are abbreviations for join
operators to be used in the L</join> method, and 
values are associated SQL clauses with placeholders
in C<sprintf> format. The default is described
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


=item multicols_sep

A string or compiled regular expression used as a separator for
"multicolumns". This separator can then be used on the left-hand side
and right-hand side of an C<IN> operator, like this :

  my $sqla = SQL::Abstract::More->new(multicols_sep => '/');
  ($sql, @bind) = $sqla->select(
   -from     => 'Foo',
   -where    => {"x/y/z" => {-in => ['1/A/foo', '2/B/bar']}},
  );

Alternatively, tuple values on the right-hand side can also be given
as arrayrefs instead of plain scalars with separators :

   -where    => {"x/y/z" => {-in => [[1, 'A', 'foo'], [2, 'B', 'bar']]}},

but the left-hand side must stay a plain scalar because an array reference
wouldn't be a proper key for a Perl hash; in addition, the presence
of the separator in the string is necessary to trigger the special
algorithm for multicolumns.

The generated SQL depends on the boolean flag C<has_multicols_in_SQL>,
as explained in the next paragraph.

=item has_multicols_in_SQL

A boolean flag that controls which kind of SQL will be generated for
multicolumns. If the flag is B<true>, this means that the underlying DBMS
supports multicolumns in SQL, so we just generate tuple expressions.
In the example from the previous paragraph, the SQL and bind values
would be :

   # $sql  : "WHERE (x, y, z) IN ((?, ?, ?), (?, ?, ?))"
   # @bind : [ qw/1 A foo 2 B bar/ ]

It is also possible to use a subquery, like this :

  ($sql, @bind) = $sqla->select(
   -from     => 'Foo',
   -where    => {"x/y/z" => {-in => \[ 'SELECT (a, b, c) FROM Bar '
                                       . 'WHERE a > ?', 99]}},
  );
  # $sql  : "WHERE (x, y, z) IN (SELECT (a, b, c) FROM Bar WHERE a > ?)"
  # @bind : [ 99 ]

If the flag is B<false>, the condition on tuples will be
automatically converted using boolean logic :

   # $sql  : "WHERE (   (x = ? AND y = ? AND z = ?) 
                     OR (x = ? AND y = ? AND z = ?))"
   # @bind : [ qw/1 A foo 2 B bar/ ]

If the flag is false, subqueries are not allowed.


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
    -union    => [ %select_subargs ], # OR -intersect, -minus, etc
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


=item C<< -where => $criteria >>

Like in L<SQL::Abstract>, C<< $criteria >> can be 
a plain SQL string like C<< "col1 IN (3, 5, 7, 11) OR col2 IS NOT NULL" >>;
but in most cases, it will rather be a reference to a hash or array of
conditions that will be translated into SQL clauses, like
for example C<< {col1 => 'val1', col2 => 'val2'} >>.
The structure of that hash or array can be nested to express complex
boolean combinations of criteria; see
L<SQL::Abstract/"WHERE CLAUSES"> for a detailed description.

When using hashrefs or arrayrefs, leaf values can be "bind values with types";
see the L</"BIND VALUES WITH TYPES"> section below.

=item C<< -union => [ %select_subargs ] >>

=item C<< -union_all => [ %select_subargs ] >>

=item C<< -intersect => [ %select_subargs ] >>

=item C<< -except => [ %select_subargs ] >>

=item C<< -minus => [ %select_subargs ] >>

generates a compound query using set operators such as C<UNION>,
C<INTERSECT>, etc. The argument C<%select_subargs> contains a nested
set of parameters like for the main select (i.e. C<-columns>,
C<-from>, C<-where>, etc.); however, arguments C<-columns> and
C<-from> can be omitted, in which case they will be copied from the
main select(). Several levels of set operators can be nested.

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

Column names C<asc> and C<desc> are treated as exceptions to this
rule, in order to preserve compatibility with L<SQL::Abstract>.
So C<< -orderBy => [-desc => 'colA'] >> yields
C<< ORDER BY colA DESC >> and not C<< ORDER BY desc DEC, colA >>.
Any other syntax supported by L<SQL::Abstract> is also
supported here; see L<SQL::Abstract/"ORDER BY CLAUSES"> for examples.

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

Like for L</select>, values assigned to columns can have associated
SQL types; see L</"BIND VALUES WITH TYPES">.

Named parameters to the C<insert()> method are just syntactic sugar
for better readability of the client's code. Parameters
C<-into> and C<-values> are passed verbatim to the parent method.
Parameter C<-returning> is optional and only
supported by some database vendors (see L<SQL::Abstract/insert>);
if the C<$return_structure> is 

=over

=item *

a scalar or an arrayref, it is passed directly to the parent method

=item *

a hashref, it is interpreted as a SQL clause "RETURNING .. INTO ..",
as required in particular by Oracle. Hash keys are field names, and
hash values are references to variables that will receive the
results. Then it is the client code's responsibility
to use L<DBD::Oracle/bind_param_inout> for binding the variables
and retrieving the results, but the L</bind_params> method in the
present module is there for help. Example:

  ($sql, @bind) = $sqla->insert(
    -into      => $table,
    -values    => {col => $val, ...},
    -returning => {key_col => \my $generated_key},
  );

  my $sth = $dbh->prepare($sql);
  $sqla->bind_params($sth, @bind);
  $sth->execute;
  print "The new key is $generated_key";

=back


=head2 update

  # positional parameters, directly passed to the parent class
  ($sql, @bind) = $sqla->update($table, \%fieldvals, \%where);

  # named parameters, handled in this class
  ($sql, @bind) = $sqla->update(
    -table    => $table,
    -set      => {col => $val, ...},
    -where    => \%conditions,
    -order_by => \@order,
    -limit    => $limit,
  );

This works in the same spirit as the L</insert> method above.
Positional parameters are supported for backwards compatibility
with the old API; but named parameters should be preferred because
they improve the readability of the client's code.

Few DBMS would support parameters C<-order_by> and C<-limit>, but
MySQL does -- see L<http://dev.mysql.com/doc/refman/5.6/en/update.html>.


=head2 delete

  # positional parameters, directly passed to the parent class
  ($sql, @bind) = $sqla->delete($table, \%where);

  # named parameters, handled in this class 
  ($sql, @bind) = $sqla->delete (
    -from     => $table
    -where    => \%conditions,
    -order_by => \@order,
    -limit    => $limit,

  );

Positional parameters are supported for backwards compatibility
with the old API; but named parameters should be preferred because
they improve the readability of the client's code.

Few DBMS would support parameters C<-order_by> and C<-limit>, but
MySQL does -- see L<http://dev.mysql.com/doc/refman/5.6/en/update.html>.

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

  my $join_info = $sqla->join(
    <table0> <join_1> <table_1> ... <join_n> <table_n>
  );
  my $sth = $dbh->prepare($join_info->{sql});
  $sth->execute(@{$join_info->{bind}})
  while (my ($alias, $aliased) = each %{$join_info->{aliased_tables}}) {
    say "$alias is an alias for table $aliased";
  }

Generates join information for a JOIN clause, taking as input
a collection of joined tables with their join conditions.
The following example gives an idea of the available syntax :

  ($sql, @bind) = $sqla->join(qw[
     Table1|t1       ab=cd                     Table2|t2
                 <=>{ef>gh,ij<kl,mn='foobar'}  Table3
                  =>{t1.op=qr}                 Table4
     ]);

This will generate

  Table1 AS t1 INNER JOIN Table2 AS t2 ON t1.ab=t2.cd
               INNER JOIN Table3       ON t2.ef>Table3.gh
                                      AND t2.ij<Table3.kl
                                      AND t2.mn=?
                LEFT JOIN Table4       ON t1.op=Table4.qr

with one bind value C<foobar>.

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

The join conditions are a comma-separated list
of binary column comparisons, like for example

  {ab=cd,Table1.ef<Table2.gh}

Table names may be explicitly given using dot notation,
or may be implicit, in which case they will be filled
automatically from the names of operands on the
left-hand side and right-hand side of the join.

Strings within quotes will be treated as bind values instead
of column names; pairs of quotes within such values become
single quotes. Ex.

  {ab=cd,ef='foo''bar',gh<ij}

becomes

  ON Table1.ab=Table2.cd AND Table1.ef=? AND Table1.gh<Table2.ij
  # bind value: "foo'bar"

In accordance with L<SQL::Abstract> common conventions,
if the list of comparisons is within curly braces, it will
become an C<AND>; if it is within square brackets, it will
become an C<OR>.

Join specifications expressed as strings
are converted into internal hashrefs with keys
C<operator> and C<condition>, like this :

  {
    operator  => '<=>',
    condition => { '%1$s.ab' => {'=' => {-ident => '%2$s.cd'}},
                   '%1$s.ef' => {'=' => {-ident => 'Table2.gh'}}},
  }

The C<operator> is a key into the C<join_syntax> table; the associated
value is a C<sprintf> format string, with placeholders for the left and
right operands, and the join condition.  The C<condition> is a
structure suitable for being passed as argument to
L<SQL::Abstract/where>.  Places where the names of left/right tables
(or their aliases) are expected should be expressed as C<sprintf>
placeholders, i.e.  respectively C<%1$s> and C<%2$s>. Usually the
right-hand side of the condition refers to a column of the right
table; in such case it should B<not> belong to the C<@bind> list, so
this is why we need to use the C<-ident> operator from
L<SQL::Abstract>. Only when the right-hand side is a string constant
(string within quotes) does it become a bind value : for example

  ->join(qw/Table1 {ab=cd,ef='foobar'}) Table2/)

is parsed into

  [ 'Table1',
    { operator  => '<=>',
      condition => { '%1$s.ab' => {'=' => {-ident => '%2$s.cd'}},
                     '%1$s.ef' => {'=' => 'foobar'} },
    },
    'Table2',
  ]


Hashrefs for join specifications as shown above can be passed directly
as arguments, instead of the simple string representation.

=head3 Return value

The structure returned by C<join()> is a hashref with 
the following keys :

=over

=item sql

a string containing the generated SQL

=item bind

an arrayref of bind values

=item aliased_tables

a hashref where keys are alias names and values are names of aliased tables.

=back


=head2 merge_conditions

  my $conditions = $sqla->merge_conditions($cond_A, $cond_B, ...);

This utility method takes a list of "C<where>" conditions and
merges all of them in a single hashref. For example merging

  ( {a => 12, b => {">" => 34}}, 
    {b => {"<" => 56}, c => 78} )

produces 

  {a => 12, b => [-and => {">" => 34}, {"<" => 56}], c => 78});


=head2 bind_params

  $sqla->bind_params($sth, @bind);

For each C<$value> in C<@bind>:

=over 

=item *

if the value is a scalarref, call

  $sth->bind_param_inout($index, $value, $INOUT_MAX_LEN)

(see L<DBI/bind_param_inout>). C<$INOUT_MAX_LEN> defaults to
99, which should be good enough for most uses; should you need another value, 
you can change it by setting

  local $SQL::Abstract::More::INOUT_MAX_LEN = $other_value;

=item *

if the value is an arrayref that matches L</is_bind_value_with_type>,
then call the method and arguments returned by L</is_bind_value_with_type>.

=item *

for all other cases, call

  $sth->bind_param($index, $value);

=back

This method is useful either as a convenience for Oracle
statements of shape C<"INSERT ... RETURNING ... INTO ...">
(see L</insert> method above), or as a way to indicate specific
datatypes to the database driver.

=head2 is_bind_value_with_type

  my ($method, @args) = $sqla->is_bind_value_with_type($value);


If C<$value> is a ref to a pair C<< [\%args, $orig_value] >> :


=over 

=item *

if  C<%args> is of shape C<< {dbd_attrs => \%sql_type} >>,
then return C<< ('bind_param', $orig_value, \%sql_type) >>.

=item *

if  C<%args> is of shape C<< {sqlt_size => $num} >>,
then return C<< ('bind_param_inout', $orig_value, $num) >>.

=back

Otherwise, return C<()>.



=head1 BIND VALUES WITH TYPES

At places where L<SQL::Abstract> would expect a plain value,
C<SQL::Abstract::More> also accepts a pair, i.e. an arrayref of 2
elements, where the first element is a type specification, and the
second element is the value. This is convenient when the DBD driver needs
additional information about the values used in the statement.

The usual type specification is a hashref C<< {dbd_attrs => \%type} >>,
where C<\%type> is passed directly as third argument to
L<DBI/bind_param>, and therefore is specific to the DBD driver.

Another form of type specification is C<< {sqlt_size => $num} >>,
where C<$num> will be passed as buffer size to L<DBI/bind_param_inout>.

Here are some examples

  ($sql, @bind) = $sqla->insert(
   -into   => 'Foo',
   -values => {bar => [{dbd_attrs => {ora_type => ORA_XMLTYPE}}]},
  );
  ($sql, @bind) = $sqla->select(
   -from  => 'Foo',
   -where => {d_begin => {">" => [{dbd_attrs => {ora_type => ORA_DATE}}, 
                                  $some_date]}},
  );


When using this feature, the C<@bind> array will contain references
that cannot be passed directly to L<DBI> methods; so you should use
L</bind_params> from the present module to perform the appropriate
bindings before executing the statement.


=head1 TODO

Future versions may include some of these features :

=over

=item *

support for C<WITH> initial clauses, and C<WITH RECURSIVE>.

=item *

support for Oracle-specific syntax for recursive queries
(START_WITH, PRIOR, CONNECT_BY NOCYCLE, CONNECT SIBLINGS, etc.)

=item *

support for INSERT variants

    INSERT .. DEFAULT VALUES
    INSERT .. VALUES(), VALUES()

=item *

support for MySQL C<LOCK_IN_SHARE_MODE>

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

=item RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=SQL-Abstract-More>

=item AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/SQL-Abstract-More>

=item CPAN Ratings

L<http://cpanratings.perl.org/d/SQL-Abstract-More>

=item MetaCPAN

L<https://metacpan.org/module/SQL::Abstract::More>

=back


=head1 LICENSE AND COPYRIGHT

Copyright 2011-2015 Laurent Dami.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut


