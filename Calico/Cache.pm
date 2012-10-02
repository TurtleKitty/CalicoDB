package Calico::Cache;

# This package exists just in case I need to improve performance
# some day with shared memory or memcached.  Seems unlikely at
# the moment, but you nevah know.


use strict;


my %stash;


sub create {
  my ($class, $namespace) = @_;

  unless ( defined( $stash{$namespace} ) ) {
    $stash{$namespace} = bless({ }, $class);
  }

  return($stash{$namespace});
}


sub get {
  my ($this, $key) = @_;
  return($this->{$key});
}


sub set {
  my ($this, $key, $value) = @_;
  $this->{$key} = $value;
  return;
}


sub delete {
  my ($this, $key) = @_;
  delete($this->{$key});
  return;
}


sub destroy {
  my ($this) = @_;
  undef($this);
  return;
}


1;

