#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use TA::ObjReg;
use TA::Hasher;
use TA::Util;

my @PAR= ();
my $project;
my $subproject;

while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PAR, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--/)
  {
       if ($arg eq '--project')    {    $project= shift (@ARGV); }
    elsif ($arg eq '--subproject') { $subproject= shift (@ARGV); }
  }
  elsif ($arg =~ /^-/)
  {
    my @a= split ('|', $arg);
    foreach my $a (@a)
    {
         if ($a eq 'p') { $project= shift (@ARGV); }
      elsif ($a eq 's') { $subproject= shift (@ARGV); }
    }
  }
  else { push (@PAR, $arg); }
}

unless (defined ($project))
{
  &usage ('no project specified');
}

my $objreg= new TA::ObjReg ('project' => $project, 'subproject' => $subproject);
print "objreg: ", Dumper ($objreg);

my $catalog=  $objreg->{'cfg'}->{'catalog'};
unless (defined ($catalog))
{
  &usage ('no catalog specified');
}

if ($catalog->{'format'} eq 'md5cat')
{
  refresh_md5cat ($objreg, 'subproject' => $subproject);
}

print "objreg: (after refresh)", Dumper ($objreg);

exit (0);

sub refresh_md5cat
{
  my $objreg= shift;
  my %extra= @_;

  # my $catalog=  $objreg->{'cfg'}->{'catalog'};
  system ('/usr/local/bin/chkmd5.pl');

  # my $hasher= $objreg->{'hasher'};

  open (CAT, '_catalog') or die "cant read catalog";
  while (<CAT>)
  {
    chop;
    my ($md5, $xf, $size, $path)= split (' ', $_, 4);
    $path=~ s#^\.\/##;
    print "md5=[$md5] size=[$size] path=[$path]\n";
    $objreg->add ($md5, 0, { 'md5' => $md5, 'size' => $size, 'path' => $path, %extra } );
  }
  close (CAT);
}


sub usage
{
  my $msg= shift;
  print $msg, "\n";
  system ("perldoc $0");
  exit -1;
}

