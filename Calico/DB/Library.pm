package Calico::DB::Library;

use strict;

use Calico::Cache;
use Calico::DB::Connection;


# This module is the lowest-level database abstraction atop DBI.
# Don't use this directly - use Calico::DB::Set and Calico::DB::Record
# for a high-level (SQL free) interface.

# create({ })
#   create() creates an instance of this module.
#   The database connection is set up at creation.
#   The hash parameter should contain dsn, user, and pass values.

# describe($table)
#   describe() takes a table name and returns a hash of columns, pkeys, and
#   auto_increment fields.

# select({ })
#   select() takes a hashref containing a table name and a hash of where parameters
#   and returns a reference to an array of hashes, one per database row.

# insert({ })
#   insert() takes a hashref containing a table name and a hash of field => value
#   pairs, inserts the row into the database, and returns the insert id.

# update({ })
#   update() takes a hashref containing a table name and a hash of where parameters,
#   updates the row(s), and returns undef.

# delete({ })
#   delete() takes a hashref containing a table name and a hash of where parameters,
#   deletes the row(s), and returns undef.

# where({})
#   where() is for internal use.  It takes a hash of field => value pairs and returns
#   a hash containing 'clause': a string like 'field1 = ?, field2 = ?' and 'params',
#   which are passed to DBI to fill in the placeholders

# query({ sql => $, attrz => { }, params => [ ] })
#   query is a direct SQL interface for more complicated queries than simple selects.



my %stash;

my $cache_namespace = '/calico/db/library';


sub create {
  my ($class, $dbconf) = @_;

  my $libkey = $dbconf->{dsn};

  unless ( defined( $stash{$libkey} ) ) {
    my $this = bless({}, $class);
    $this->{cache} = Calico::Cache->create($cache_namespace);
    $this->{connection} = Calico::DB::Connection->create($dbconf);
    $stash{$libkey} = $this;
  }

  return($stash{$libkey});
}


sub describe {
  my ($this, $table) = @_;

  # describe fields: Field Type Null Key Default Extra

  my $desc = $this->{cache}->get($table);

  if ( defined($desc) ) {
    return($desc);
  }

  my $sql  = qq{ describe $table };

  my $rows = $this->{connection}->read({
    sql    => $sql,
    params => []
  });

  $desc = {
    pkeys   => [ ],
    columns => { },
  };

  for my $r (@{$rows}) {
    my $field = $r->{field};

    $desc->{columns}->{$field} = $r;

    if ($r->{key} eq 'PRI') {
      push(@{$desc->{pkeys}}, $field);
    }

    if ($r->{extra} =~ m/auto_increment/i) {
      $desc->{ai} = $field;
    }
  }

  $this->{cache}->set($table, $desc);

  return($desc);
}


sub select {
  my ($this, $args) = @_;

  my $table = $args->{table};
  
  my $where = $this->where($args->{where});

  my $sql = qq{
    select * from $table
    $where->{clause}
  };

  my $set = $this->{connection}->read({
    sql    => $sql,
    params => $where->{params},
  });

  return($set);
}


sub insert {
  my ($this, $args) = @_;

  my (@columns, @params);

  for my $f (keys %{$args->{fields}}) {
    push(@columns, $f);
    push(@params, $args->{fields}->{$f});
  }

  my $columns   = join(',', @columns);
  my $questions = join(',', ('?') x @columns);

  my $sql = qq{
    insert into $args->{table} ($columns)
    values ($questions)
  };

  $this->{connection}->write({
    sql    => $sql,
    params => \@params,
  });

  my $id = $this->{connection}->last_insert_id();

  return($id);
}


sub update {
  my ($this, $args) = @_;

  my (@set, @params);

  for my $k (keys %{$args->{fields}}) {
    push(@set, "$k = ?");
    push(@params, $args->{fields}->{$k});
  }

  my $set_clause = join(',', @set);

  my $where = $this->where($args->{where});

  @params = (@params, @{$where->{params}});

  my $sql = qq{
    update $args->{table}
    set    $set_clause
    $where->{clause}
  };

  $this->{connection}->write({
    sql    => $sql,
    params => \@params,
  });

  return;
}


sub delete {
  my ($this, $args) = @_;

  my $where = $this->where($args->{where});

  my $sql = qq{
    delete from $args->{table}
    $where->{clause}
  };

  $this->{connection}->write({
    sql    => $sql,
    params => $where->{params},
  });

  return;
}


sub where {
  my ($this, $where) = @_;

  unless ($where) { return({ }); }

  my (@where, @params);

  for my $k (keys %{$where}) {
    push(@where, "$k = ?");
    push(@params, $where->{$k});
  }

  my $clause = 'where ' . join(' and ', @where);

  return({ clause => $clause, params => \@params });
}


sub query {
  my ($this, $args) = @_;
  my $set = $this->{connection}->read($args);
  return($set);
}


1;

