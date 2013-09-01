
package TA::ObjReg;

use strict;

use JSON;
use TA::Util;

sub new
{
  my $class= shift;
  my %par= @_;

  # check the presence of all required parameters
  my $stopit= 0;
  foreach my $k (qw(project subproject))
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

  $par;
}

sub get_project
{
  my $obj= shift;

  my $proj_name= $obj->{'project'};
  my $proj_cfg_fnm= join ('/', $ENV{'TABASE'}, 'projects', $proj_name, 'config.json');

  my $proj_cfg;
  unless ($proj_cfg= TA::Util::slurp_file ($proj_cfg_fnm, 'json'))
  {
    print STDERR "project '$proj_name' at 'proj_cfg_fnm'\n";
    return undef;
  }

}


sub add
{
}

# =head1 INTERNAL FUNCTIONS

1;
__END__

=head1 ENVIRONMENT


=head1 TODOs

* this is a stub for storage in a local filesystem
** connect to centralized connection service


