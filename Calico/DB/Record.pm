package Calico::DB::Record;

use strict;

use Calico::DB::Library;


# This module is a high-level abstraction of a single database row.
# It does not read from the database directly - use Calico::Set to
# get sets of records.

# create({}) and load({})
# ({ dbconf => { }, table => $, row => { } })
#   create() and load({}) both take a hashref containing a table name
#   and a row of data and return a Calico::DB::Record object.
#   The only difference between them is the status that the object is
#   set to - create sets status = new, load sets status = loaded.

# read({ dbconf => { }, table => $, where => { } })
#   read is a shortcut method for use when you expect one and only one
#   row to be returned from a query.

# get($key) and set($key, $value)
#   These methods do what you'd expect.  Each Calico::DB::Record keeps
#   track of it's state as new, modified, or loaded and knows which
#   fields have been altered.

# write()
#   write() inserts or updates the row and returns the object.

# delete()
#   delete() deletes the object from the database if necessary
#   and sets $this to undef.

# clone()
#   clone() creates a new Calico::DB::Record identical to the current one
#   (but with any auto_incremented fields set to undef) and returns it.


my $new      = 1;
my $loaded   = 2;
my $modified = 3;

my $exptime  = 600;


sub init {
  my ($class, $args) = @_;

  my $this = bless({}, $class);

  $this->{dbconf}  = $args->{dbconf} || die("No dbconf given.");
  $this->{table}   = $args->{table}  || die("No table given.");
  $this->{values}  = $args->{row}    || {};

  $this->{dblib}   = Calico::DB::Library->create($this->{dbconf});

  my $desc         = $args->{desc} || $this->{dblib}->describe($this->{table});

  $this->{columns} = $desc->{columns};
  $this->{pkeys}   = $desc->{pkeys};
  $this->{ai}      = $desc->{ai};

  $this->{altered} = [];

  return($this);  
}


sub create {
  my ($class, $args) = @_;
  my $this = init($class, $args);
  $this->{status} = $new;
  return($this);  
}


sub load {
  my ($class, $args) = @_;
  my $this = init($class, $args);
  $this->{status} = $loaded;
  return($this);
}


sub read {
  my ($class, $args) = @_;

  my $wherz = scalar(keys %{$args->{where}});

  unless ($wherz > 0) { 
    return;
  }

  my $this   = init($class, $args);

  my $rows = $this->{dblib}->select($args);
  my $n    = scalar(@{$rows});

  unless ($n == 1) {
    return;
  }

  $this->{values} = $rows->[0];
  $this->{status} = $loaded;

  return($this);  
}


sub write {
  my ($this) = @_;

  if ($this->is_new())      { $this->insert(); }
  if ($this->is_modified()) { $this->update(); }

  $this->{altered} = [];
  $this->{status}  = $loaded;

  return($this);
}


sub get {
  my ($this, $field) = @_;
  return($this->{values}->{$field});
}


sub set {
  my ($this, $field, $value) = @_;

  unless ($this->is_new()) {
    $this->{status} = $modified;
  }

  $this->{values}->{$field} = $value;

  push(@{$this->{altered}}, $field);

  return;
}


sub is_new {
  my ($this) = @_;
  return($this->{status} == $new);
}


sub is_modified {
  my ($this) = @_;
  return($this->{status} == $modified);
}


sub insert {
  my ($this) = @_;

  my $set;
  my @params;

  my $args = {
    table  => $this->{table},
    fields => $this->{values},
  };

  my $id = $this->{dblib}->insert($args);

  if ($this->{ai}) {
    $this->{values}->{$this->{ai}} = $id;
  }

  return;
}


sub update {
  my ($this) = @_;

  my ($fields, $where);

  for my $col (@{$this->{altered}}) {
    $fields->{$col} = $this->{values}->{$col};
  }

  for my $pk (@{$this->{pkeys}}) {
    $where->{$pk} = $this->{values}->{$pk};
  }

  my $args = {
    table  => $this->{table},
    fields => $fields,
    where  => $where,
  };

  $this->{dblib}->update($args);

  return;
}


sub delete {
  my ($this) = @_;

  unless ($this->is_new()) {
    my $where;

    for my $pk (@{$this->{pkeys}}) {
      $where->{$pk} = $this->{values}->{$pk};
    }

    my $args = {
      table  => $this->{table},
      where  => $where,
    };

    $this->{dblib}->delete($args);
  }

  undef($this);

  return;
}


sub clone {
  my ($this) = @_;

  my $row = $this->{values};

  for my $col (keys %{$this->{columns}}) {
    if ($col eq $this->{ai}) {
      $row->{$col} = undef;
    }
  }

  my $clone = Calico::DB::Record->create({
    dbconf => $this->{dbconf},
    table  => $this->{table},
    row    => $this->{values},
  });

  return($clone);
}


1;

