package Calico::DB::Connection;

use strict;

use DBI;


# create({}) returns a new Calico::Connection object.
#   the hash should contain values for dsn, user, and pass.

# read({}) takes a hashref: { sql => $, attrz => \%, params => \@ },
#   reads from the database and returns a reference to an array of
#   hashrefs, one for each row.

# write({}) takes a hashref: { sql => $, attrz => \%, params => \@ },
#   writes to the database and returns undef.

# last_insert_id() returns the id for the row of the last insert.


sub create {
  my ($class, $dbconf) = @_;

  my $this = bless({ }, $class);

  $this->{dbh} = $this->connect($dbconf);

  return($this);
}


sub connect {
  my ($this, $dbconf) = @_;

  my $attrz = {
    RaiseError         => 1,
    ShowErrorStatement => 1,
    FetchHashKeyName   => 'NAME_lc',
  };

  my $dbh = DBI->connect(
    $dbconf->{dsn},
    $dbconf->{user},
    $dbconf->{pass},
    $attrz,
  ) or die("Failed to connect to the database: $DBI::errstr");

  return($dbh);
}


sub read {
  my ($this, $args) = @_;

  my $sql    = $args->{sql};
  my $params = $args->{params} || [];

  my $sth = $this->sth($sql);

  $sth->execute(@{$params});

  my $rows = $sth->fetchall_arrayref({}) || [];

  return($rows);
}


sub write {
  my ($this, $args) = @_;

  my $sql    = $args->{sql};
  my $params = $args->{params} || [];

  my $sth = $this->sth($sql);

  $sth->execute(@{$params}); 

  return;
}


sub last_insert_id {
  my ($this) = @_;

  # mysql ignores all args, but they still have to be there. :rolleyes:

  my ($catalog, $schema, $table, $field);

  my $liid = $this->{dbh}->last_insert_id(
    $catalog,
    $schema,
    $table,
    $field,
  );

  return($liid);
}


sub sth {
  my ($this, $sql) = @_;

  my $sth = $this->{dbh}->prepare($sql);

  return($sth);
}


1;

