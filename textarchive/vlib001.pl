#!/usr/bin/perl

=head1 NAME

  vlib001.pl

=head1 USAGE

  vlib001.pl -p project-name [-s store-name] [paraemters]*

  options:
  * -p <project-name>
  * -s <store-name>
  * --verify   ... verify/create TOC structures
  * --fileinfo ... refresh file info
  * --lookup   ... lookup for hashes given as parameters
  * -D ... increase debug level

=head1 DESCRIPTION

Updates the _catalog file using md5cat scripts and registers the files in
the project's object registry.  The environment variable TABASE must point
to the directory where the object registry's configuration is stored.

=cut

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use TA::ObjReg;
# use TA::Hasher;
# use TA::Util;
use md5cat;

my @PAR= ();
my $project;
my $store;
my $refresh_fileinfo= 0;
my $DEBUG= 0;
my $STOP= 0;
my $op_mode= 'refresh';

my @hdr= qw(md5 path mtime fs_size ino);

# --- 8< --- [from chkmd5.pl] ---
# my $Dir_Pattern= '[0-9_a-zA-Z]*';
my $Dir_Pattern= '.';
my $DEFAULT_file_list= "find $Dir_Pattern -xdev -type f -print|";
# --- >8 ---

while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PAR, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--/)
  {
       if ($arg eq '--project')  { $project= shift (@ARGV); }
    elsif ($arg eq '--store')    { $store= shift (@ARGV); }
    elsif ($arg eq '--fileinfo') { $refresh_fileinfo= 1; }
    elsif ($arg =~ /^--(refresh|verify|lookup|edit)$/) { $op_mode= $1; }
  }
  elsif ($arg =~ /^-/)
  {
    my @a= split ('|', $arg);
    foreach my $a (@a)
    {
         if ($a eq 'p') { $project= shift (@ARGV); }
      elsif ($a eq 's') { $store= shift (@ARGV); }
      elsif ($a eq 'D') { $DEBUG++; }
      elsif ($a eq 'X') { $STOP= 1; }
    }
  }
  else { push (@PAR, $arg); }
}

print "debug level: $DEBUG\n";

&usage ('environment variable TABASE not set') unless (exists ($ENV{'TABASE'}));
# &usage ('no project specified') unless (defined ($project));
unless (defined ($project))
{
  print "no project specified; check these:\n";
  system ("ls -ls \"$ENV{'TABASE'}/projects\"");
  exit (0);
}
# &usage ('no store specified') unless (defined ($store));

my $objreg= new TA::ObjReg ('project' => $project, 'store' => $store, 'key' => 'md5');
&usage ('no config found') unless (defined ($objreg));
print "objreg: ", Dumper ($objreg) if ($DEBUG || $STOP);
exit if ($STOP);

if ($op_mode eq 'refresh')
{
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

     if ($catalog->{'format'} eq 'md5cat')   { refresh_md5cat   ($objreg, $store); }
  elsif ($catalog->{'format'} eq 'internal') { refresh_internal ($objreg, $store); }
}
elsif ($op_mode eq 'verify')
{
  $objreg->verify_toc (\&verify_toc_item, \@hdr);
}
elsif ($op_mode eq 'edit')
{
  print "objreg: ", Dumper ($objreg);
  my $proj_cfg_fnm= $objreg->{'proj_cfg_fnm'};
  system ("\$EDITOR '$proj_cfg_fnm'");
  # print "store_cfg: ", Dumper ($store_cfg);
}
elsif ($op_mode eq 'lookup')
{
  foreach my $key (@PAR)
  {
    my $res= $objreg->lookup ( { 'md5' => $key } );
    print "res: ", Dumper ($res);
  }
}

# print "objreg: (after refresh)", Dumper ($objreg);

exit (0);

sub usage
{
  my $msg= shift;
  print $msg, "\n";
  system ("perldoc $0");
  exit -1;
}

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

sub refresh_internal
{
  my $objreg= shift;
  my $store= shift;
  my %extra= @_;

  my $cnt_processed= 0;
  my $cnt_updated= 0;
  my $cnt_dropped= 0;

  $objreg->verify_toc (\&verify_toc_item, \@hdr);
  print "toc verfified\n";
  my $toc= $objreg->load_single_toc ($store);
  # print "toc: ", Dumper ($toc);

  my $md5cat= new md5cat ();
  $md5cat->read_flist ($DEFAULT_file_list);
  # print "md5cat: ", Dumper ($md5cat);
  print "flist processed\n";

  # compare TOC and reference filelist
  my $fl= $md5cat->{'FLIST'};
  my %key= ();
  my $cnt= 0;
  if (defined ($toc))
  {
  # print "toc: ", Dumper ($toc);
  printf ("%6d items to be processed\n", scalar @$toc);
  foreach my $x (@$toc)
  {
    printf ("%6d items processed\n", $cnt) if ((++$cnt % 100) == 0);
# print __LINE__, " k=[$k]\n";
    my $k= $x->{'key'};
    my $p= $x->{'path'};
    $key{$k}->{$p}= 0;

    if (exists ($fl->{$p}))
    {
      $cnt_processed++;
      my $f= $fl->{$p};
      my $matches= 1;
      AN: foreach my $an (qw(mtime size ino))
      {
        unless ($f->{$an} eq $x->{$an})
        {
          # print "mismatch in [$an]! x: ", Dumper ($x); print "f: ", Dumper ($f);
          $matches= 0;
          last AN;
        }
      }

# print "matches: $p $matches\n";
      if ($matches)
      {
        $f->{'state'}= 'nocheck';
        $f->{'md5'}= $x->{'md5'};
      }
    }
    else
    {
      # print "file missing: ", Dumper ($x);
      $cnt_dropped++;
    }
  }
  # my %paths= map { my $x= $toc->{$_}; $x->{'found'}= 0; $x->{'path'} => $x } keys %$toc;
  # print "paths: ", Dumper (\%paths);
  # print "fl: ", Dumper ($fl);
  }

print __LINE__, " check_new_files\n";
  my $new_files= $md5cat->check_new_files ();
  # print "new_files: ", Dumper ($new_files);
print __LINE__, " integrate_md5_sums\n";
  $md5cat->integrate_md5_sums ($new_files);
  # $md5cat->save_catalog (); # TODO: if save_catalog flag is true!

# ZZZ
  # update the Object registry with new items
  printf ("%6d new items to be processed\n", scalar @$new_files);
  foreach my $nf (@$new_files)
  {
    my ($md5, $path, $size, $mtime)= @$nf;
    # print "md5=[$md5] size=[$size] path=[$path]\n";

    $cnt_processed++;
    my @upd= process_file ($md5, $path, $size);
    $cnt_updated++ if (@upd);
  }

  # get filelist again after reintegration to find keys which are no longer in the catalog
  $fl= $md5cat->{'FLIST'};
  # print __LINE__, " fl: ", Dumper ($fl);
  foreach my $p (keys %$fl)
  {
    $key{$fl->{$p}->{'md5'}}->{$p}= 1;
  }
  # print __LINE__, " key: ", Dumper (\%key);

  my @drop= ();
  foreach my $k (keys %key)
  {
    my $x1= $key{$k};
    foreach my $p (keys %$x1)
    {
      push (@drop, [$k, $p]) if ($x1->{$p} == 0);
    }
  }
  print __LINE__, " drop: (", scalar @drop, ") ", Dumper (\@drop);

  $objreg->remove_from_store ($store, \@drop);

  printf ("files: %6d processed; %6d updated; %6d (%d) dropped\n",
          $cnt_processed, $cnt_updated, $cnt_dropped, scalar (@drop));
}

=head2 process_file

TBD
returns list of elements that where updated

=cut

sub process_file
{
  my ($md5, $path, $size)= @_;

    my @st= stat ($path);
    unless (@st)
    {
      print STDERR "ATTN: could not stat file '$path'\n";
      return undef;
    }

    my $xdata=
    {
      # 'key' => $md5, 'key_type' => 'md5',
      'store' => $store,
      'c_size' => $size, 'path' => $path, 'md5' => $md5,
      'mtime' => $st[9], 'fs_size' => $st[7], 'ino' => $st[1]
    };

    my $search= { 'md5' => $md5, 'store' => $store, 'path' => $path };
    my $reg= $objreg->lookup ($search);
    # print __LINE__, " reg: ", Dumper ($reg);

    my @upd;
    my $ydata;   # pointer to file catalog data within main datastructure
    if (defined ($reg))
    { # we know something about this key value but not in respect to the repository at hand
      # print "json read: ", main::Dumper ($reg);
        foreach my $an (keys %$xdata)
        {
          unless ($reg->{$an} eq $xdata->{$an})
          {
            $reg->{$an}= $xdata->{$an};
            push (@upd, $an);
          }
        }
    }
    else
    { # this key is new, so we simply place what we know in the newly created registry item
      # $reg= { 'key' => $md5, 'key_type' => 'md5', 'store' => { $store => $ydata= $xdata } };
      $reg= $xdata;
      push (@upd, 'new key');
    }

    # fill in some more information about that file
    if (!exists ($reg->{'fileinfo'}) || $refresh_fileinfo)
    {
      my $xpath= $path;
      $xpath=~ s#'#'\\''#g;
      my $res= `/usr/bin/file '$xpath'`;
      chop ($res);

      my ($xpath, $fileinfo)= split (/: */, $res, 2);
      $reg->{'fileinfo'}= $fileinfo;
      push (@upd, 'fileinfo updated');
    }

    # TODO: some more information would probably be nice as well
    # e.g. mp3info or stuff

  if (@upd)
  {
    # print "saving (", join ('|', @upd), ")\n";
    # print __LINE__, " reg: ", Dumper ($reg);
    $objreg->save ($search, $reg);
  }

  (wantarray) ? @upd : \@upd;
}

# callback function for TA::ObjReg::verify
sub verify_toc_item
{
  my $j= shift;     # currently not used, that's the complete json entry for this item
  my $jj= shift;    # this is just the part refering to the store currently processed
  my $ster= shift;  # TOC item to be updated

# print __LINE__, " verify_toc_item: j=", Dumper ($j);
# print __LINE__, " verify_toc_item: jj=", Dumper ($jj);
  # my @paths= keys %{$jj->{'path'}};
  # $ster->{'path_count'}= scalar @paths;  ... we don't see that this way anymore

  foreach my $k (qw(md5 path mtime fs_size ino))
  {
    $ster->{$k}= $jj->{$k};
  }
}

__END__

=head1 TODO

=head2 auto-check-mode

  * The project's config contains all the information that is needed to
    locate all the stores on a given machine, so there should be an option
    that updates everything.
  * specifing the store should be optional.

=head2 misc

  * maybe it makes sense to offer an option to perform backups along the
    way, for instance, when the store is actually a git repository.
  * also, checking the VCS status (if not committing updates)
    might be useful.
  * other hashing algorithms:
    * currently we use md5 for hashing, however, this could should
      be fairly simple to adopt for sha1, sha256 or something else.
    * possibly, it makes sense to allow several hashing algorithms
      in parallel, however, then it might be a good idea to store
      file metadata in one place and let other hashes point to that
      place.

