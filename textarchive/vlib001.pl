#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use TA::ObjReg;
# use TA::Hasher;
# use TA::Util;

my @PAR= ();
my $project;
my $store= '<none>';
my $refresh_fileinfo= 0;
my $DEBUG= 0;

while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PAR, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--/)
  {
       if ($arg eq '--project')    {    $project= shift (@ARGV); }
    elsif ($arg eq '--store') { $store= shift (@ARGV); }
    elsif ($arg eq '--fileinfo') { $refresh_fileinfo= 1; }
  }
  elsif ($arg =~ /^-/)
  {
    my @a= split ('|', $arg);
    foreach my $a (@a)
    {
         if ($a eq 'p') { $project= shift (@ARGV); }
      elsif ($a eq 's') { $store= shift (@ARGV); }
      elsif ($a eq 'D') { $DEBUG++; }
    }
  }
  else { push (@PAR, $arg); }
}

print "debug level: $DEBUG\n";

&usage ('no project specified') unless (defined ($project));
# &usage ('no store specified') unless (defined ($store));

my $objreg= new TA::ObjReg ('project' => $project, 'store' => $store);
&usage ('no config found') unless (defined ($objreg));
print "objreg: ", Dumper ($objreg) if ($DEBUG);

my $catalog= $objreg->{'cfg'}->{'catalog'};
&usage ('no catalog found in config') unless (defined ($catalog));

my $stores_p= $objreg->{'cfg'}->{'stores'};
my $store_cfg= $stores_p->{$store};
unless (defined ($store_cfg))
{
  print "no store config found for '$store', check these: ", Dumper ($stores_p);
  exit (-2);
}
print "store_cfg: ", Dumper ($store_cfg) if ($DEBUG);

if ($catalog->{'format'} eq 'md5cat')
{
  refresh_md5cat ($objreg, $store);
}

# print "objreg: (after refresh)", Dumper ($objreg);

exit (0);

sub refresh_md5cat
{
  my $objreg= shift;
  my $store= shift;
  my %extra= @_;

  # my $catalog=  $objreg->{'cfg'}->{'catalog'};
  system ('/usr/local/bin/chkmd5.pl');

  # my $hasher= $objreg->{'hasher'};

  open (CAT, '<:utf8', '_catalog') or die "cant read catalog";
  my $cnt_processed= 0;
  my $cnt_updated= 0;
  CAT: while (<CAT>)
  {
    chop;
    my ($md5, $xf, $size, $path)= split (' ', $_, 4);

    $path=~ s#^\.\/##;
    # print "md5=[$md5] size=[$size] path=[$path]\n";

    $cnt_processed++;
    my @upd= process_file ($md5, $path, $size);
    $cnt_updated++ if (@upd);
  }
  close (CAT);
  printf ("%6d files processed; %6d files updated\n", $cnt_processed, $cnt_updated);
}

sub process_file
{
  my ($md5, $path, $size)= @_;

    my @st= stat ($path);
    unless (@st)
    {
      print STDERR "ATTN: could not stat file '$path'\n";
      return undef;
    }

    my $xdata= { 'c_size' => $size, 'path' => $path, 'mtime' => $st[9], 'fs_size' => $st[7] };

    my $reg= $objreg->lookup ($md5);

    my @upd;
    my $ydata;   # pointer to file catalog data within main datastructure
    if (defined ($reg))
    { # we know something about this hash value but not in respect to the repository at hand
      # print "json read: ", main::Dumper ($reg);
      my $sb;
      if (defined ($sb= $reg->{'store'}->{$store})
          && exists ($sb->{'path'})
          && defined ($ydata= $sb->{'path'}->{$path}) # we need to keep track of the path as well otherwise we can't handly duplicates in the same store
          && $st[7] == $ydata->{'fs_size'}
	  && $st[9] == $ydata->{'mtime'}
	 )
      { # TODO: compare stored and current information
      }
      else
      {
        # print "st: fs_size(7)=[$st[7]] mtime(9)=[$st[9]]\n";
        # print "ydata: ", Dumper ($ydata);
        # print "xdata: ", Dumper ($xdata);

        $reg->{'store'}->{$store}->{'path'}->{$path}= $ydata= $xdata;
        # print __LINE__, " reg: ", Dumper ($reg);
        # print "ydata: ", Dumper ($ydata);
        # print "xdata: ", Dumper ($xdata);

	push (@upd, 'store upd');
      }
    }
    else
    {
      $reg= { 'md5' => $md5, 'store' => { $store => { 'path' => { $path => $ydata= $xdata } } } };
      push (@upd, 'new md5');
    }

    # fill in some more information about that file
    if (!exists ($ydata->{'fileinfo'}) || $refresh_fileinfo)
    {
      my $xpath= $path;
      $xpath=~ s#'#'\\''#g;
      my $res= `/usr/bin/file '$xpath'`;
      chop ($res);
      my ($xpath, $fileinfo)= split (/: */, $res, 2);
      $ydata->{'fileinfo'}= $fileinfo;
      push (@upd, 'fileinfo updated');
    }
    # TODO: some more information would probably be nice as well
    # e.g. mp3info or stuff

  if (@upd)
  {
    print "saving (", join ('|', @upd), ")\n";
    # print __LINE__, " reg: ", Dumper ($reg);
    $objreg->save ($md5, $reg);
  }

  (wantarray) ? @upd : \@upd;
}

sub usage
{
  my $msg= shift;
  print $msg, "\n";
  system ("perldoc $0");
  exit -1;
}

