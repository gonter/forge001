#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use TA::ObjReg;
# use TA::Hasher;
# use TA::Util;

my @PAR= ();
my $project;
my $subproject;
my $refresh_fileinfo= 0;

while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PAR, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--/)
  {
       if ($arg eq '--project')    {    $project= shift (@ARGV); }
    elsif ($arg eq '--subproject') { $subproject= shift (@ARGV); }
    elsif ($arg eq '--fileinfo') { $refresh_fileinfo= 1; }
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
  refresh_md5cat ($objreg, $subproject);
}

# print "objreg: (after refresh)", Dumper ($objreg);

exit (0);

sub refresh_md5cat
{
  my $objreg= shift;
  my $subproject= shift;
  my %extra= @_;

  # my $catalog=  $objreg->{'cfg'}->{'catalog'};
  system ('/usr/local/bin/chkmd5.pl');

  # my $hasher= $objreg->{'hasher'};

  open (CAT, '_catalog') or die "cant read catalog";
  CAT: while (<CAT>)
  {
    chop;
    my ($md5, $xf, $size, $path)= split (' ', $_, 4);

    $path=~ s#^\.\/##;
    print "md5=[$md5] size=[$size] path=[$path]\n";
    my @st= stat ($path);

    unless (@st)
    {
      print STDERR "ATTN: could not stat file '$path'\n";
      next CAT;
    }

    my $xdata= { 'c_size' => $size, 'path' => $path, 'mtime' => $st[9], 'fs_size' => $st[7] };
    # $objreg->add ($md5, 0, { 'md5' => $md5, $subproject => { 'size' => $size, 'path' => $path, %extra } } );

    my $reg= $objreg->lookup ($md5);
    print "json read: ", main::Dumper ($reg);

    my $upd= 0;
    my $ydata;   # pointer to file catalog data within main datastructure
    if (defined ($reg))
    { # we know something about this hash value but not in respect to the repository at hand
      if (defined ($ydata= $reg->{$subproject})
          && $st[7] == $ydata->{'fs_size'}
	  && $st[9] == $ydata->{'mtime'}
	 )
      { # TODO: compare stored and current information
      }
      else
      {
        $ydata= $reg->{$subproject}= $xdata;
	$upd= 1;
      }
    }
    else
    {
      $reg= { 'md5' => $md5, $subproject => $ydata= $xdata };
      $upd= 1;
    }

    # fill in some more information about that file
    if (!exists ($ydata->{'fileinfo'}) || $refresh_fileinfo)
    {
      my $res= `/usr/bin/file '$path'`;
      chop ($res);
      my ($xpath, $fileinfo)= split (/: */, $res, 2);
      $ydata->{'fileinfo'}= $fileinfo;
      $upd= 1;
    }
    # TODO: some more information would probably be nice as well
    # e.g. mp3info or stuff

    $objreg->save ($md5, $reg) if ($upd);
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

