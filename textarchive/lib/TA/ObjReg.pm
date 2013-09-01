
package TA::ObjReg;

use strict;

use JSON;

use TA::Util;
use TA::Hasher;

sub new
{
  my $class= shift;
  my %par= @_;

  # check the presence of all required parameters
  my $stopit= 0;
  foreach my $k (qw(project store))
  {
    unless (exists ($par{$k}))
    {
      print STDERR "missing parameter '$k'\n";
      $stopit= 1;
    }
  }
  return undef if ($stopit);

  my $obj= {};
  bless $obj, $class;
  foreach my $par (keys %par)
  {
    $obj->{$par}= $par{$par};
  }

  my $cfg= $obj->get_project ();
  return undef unless (defined ($cfg));
  $obj->{cfg}= $cfg;

  $obj;
}

sub get_project
{
  my $obj= shift;

  my $proj_name= $obj->{'project'};
  $obj->{'proj_cfg_dir'}= my $proj_cfg_dir= join ('/', $ENV{'TABASE'}, 'projects', $proj_name);
  $obj->{'proj_cfg_fnm'}= my $proj_cfg_fnm= join ('/', $proj_cfg_dir, 'config.json');

  my $proj_cfg;
  unless ($proj_cfg= TA::Util::slurp_file ($proj_cfg_fnm, 'json'))
  {
    print STDERR "no project '$proj_name' at '$proj_cfg_fnm'\n";
    return undef;
  }

  # print "proj_cfg: ", main::Dumper ($proj_cfg);
  # TODO: check authorization (no need, if local, but for client-server, we need something1

  my $base_dir= $obj->{'proj_cfg_dir'};
  $obj->{'proj_cat'}= my $proj_cat= join ('/', $base_dir, 'cat');
  $obj->{'hasher'}= my $hasher= new TA::Hasher ('algorithm' => $proj_cfg->{'algorithm'}, 'pfx' => $proj_cat, 'name' => 'file');

  $proj_cfg;
}

sub lookup
{
  my $obj= shift;
  my $id_str= shift;

  # print "lookup [$id_str]\n";
  my @r= $obj->{'hasher'}->check_file ($id_str, 0);
  # print "id_str=[$id_str] r=", main::Dumper (\@r);
  my ($rc, $path)= @r;

  my $fnm= $path . '/' . $id_str . '.json';
  # print "description: [$fnm]\n";

  my @st= stat ($fnm);
  return undef unless (@st);

  my $reg= TA::Util::slurp_file ($fnm, 'json');
 
  return $reg;
}

sub save
{
  my $obj= shift;
  my $id_str= shift;
  my $new_reg= shift;

  print "save [$id_str]\n";
  my @r= $obj->{'hasher'}->check_file ($id_str, 1);
  # print "id_str=[$id_str] r=", main::Dumper (\@r);
  my ($rc, $path)= @r;

  my $fnm= $path . '/' . $id_str . '.json';
  # print "description: [$fnm]\n";

  my $j= encode_json ($new_reg);
  # print "generated json: [$j]\n";
  open (J, '>:utf8', $fnm); print J $j; close (J);
}

# =head1 INTERNAL FUNCTIONS

1;
__END__

=head1 ENVIRONMENT

=head1 TODOs

* this is a stub for storage in a local filesystem
** connect to centralized connection service


