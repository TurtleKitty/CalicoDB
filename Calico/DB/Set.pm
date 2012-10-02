package Calico::DB::Set;

use strict;

use Calico::DB::Library;
use Calico::DB::Record;


# This module is used for reading or writing many database records at once.
# Though instantiated as an object, it maintains no state; it's essentially
# procedural.  read() is the only necessary method; write() and purge() are
# merely convenience methods to save me from having for() loops everywhere.

# create({ })
#   create returns a Calico::DB::Set object.
#   The hash parameter should contain dsn, user, and pass values.

# read({ table => $, where => % })
#   read takes a hashref containing the name of a table and a hash of
#   where parameters (i.e. id => 2, name => 'jerk')
#   and returns a reference to an array of Calico::DB::Record objects.

# write([ Calico::DB::Record, Calico::DB::Record, ... ])
#   write takes a reference to an array of Calico::DB::Record objects and
#   tells each object to write itself.

# purge([ Calico::DB::Record, Calico::DB::Record, ... ])
#   purge takes a reference to an array of Calico::DB::Record objects and
#   tells each object to delete itself.

# query({ sql => $, attrz => { }, params => [ ] })
#   query is a direct SQL interface for more complicated queries than simple selects.



sub create {
  my ($class, $dbconf) = @_;

  my $this = bless({}, $class);

  $this->{dbconf} = $dbconf;
  $this->{dblib}  = Calico::DB::Library->create($this->{dbconf});

  return($this);
}


sub read {
  my ($this, $args) = @_;

  my $table = $args->{table};
  my $set   = $this->{dblib}->select($args);

  my @records;

  for my $row (@{$set}) {
    my $record = Calico::DB::Record->load({
      dbconf => $this->{dbconf},
      table  => $table,
      row    => $row,
    });

    push(@records, $record);
  }

  return(\@records);
}


sub write {
  my ($this, $records) = @_;

  for my $rec (@{$records}) {
    $rec->write();
  }

  return;
}


sub purge {
  my ($this, $records) = @_;

  for my $rec (@{$records}) {
    $rec->delete();
  }

  return;
}


sub query {
  my ($this, $args) = @_;
  my $set = $this->{dblib}->query($args);
  return($set);
}


1;

