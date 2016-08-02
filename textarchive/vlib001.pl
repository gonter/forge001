#!/usr/bin/perl

=head1 NAME

  vlib001.pl

=head1 VERSION

  Version: 0.50
  Date: Fri Oct 16 13:09:17 CEST 2015

=head1 USAGE

  vlib001.pl -p project-name [-s store-name] [parameters]*

  options:
  * -p <project-name>
  * -s <store-name>
  * --project <project-name>
  * --store <store-name>
  * --subdir <directory>
  * --verify   ... verify/create TOC structures (not for MongoDB)
  * --fileinfo ... refresh file info
  * --lookup   ... lookup for hashes given as parameters
  * --limit <n> ... check up <n> files
  * --noinode   ... ignore the inode
  * --edit      ... edit configuration
  * --cd        ... change directory to store's base
  * -D ... increase debug level
  * -X ... stop after setup (useful as option -DX)

=head1 DESCRIPTION

Updates a file catalog using md5cat library methods and registers the
files in the project's object registry.  The environment variable TABASE
must point to the directory where the object registry's configuration
is stored.

=head1 BACKENDS

Currently two backends are available and mostly supported.  The older
filesystem based TA::Hasher was more or less replaced by MongoDB.

=head2 MongoDB

The object registry is stored in a MongoDB.

=head2 TA::Hasher

Filesystem based storage backend for object information in JSON format

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
my $limit= undef;
my $show_every= 1000;
my $cat_file= '_catalog';
my $ino_file= '_catalog.inodes';
my $check_inode= 1;
my $cd_mode= 0;
my $EDITOR= $ENV{'EDITOR'} || '/bin/vi';

my @hdr= qw(md5 path mtime fs_size ino);

# --- 8< --- [from chkmd5.pl] ---
# my $Dir_Pattern= '[0-9_a-zA-Z]*';
my $Dir_Pattern= '.';
my $DEFAULT_file_list= "find $Dir_Pattern -xdev -type f -print|";
# --- >8 ---

my @subdirs= ();

while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PAR, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($opt, $val)= split ('=', $1, 2);

       if ($opt eq 'project')  { $project= $val || shift (@ARGV); }
    elsif ($opt eq 'store')    { $store=   $val || shift (@ARGV); }
    elsif ($opt eq 'limit')    { $limit=   $val || shift (@ARGV) ; }
    elsif ($opt eq 'fileinfo') { $refresh_fileinfo= 1; }
    elsif ($opt eq 'noinode')  { $check_inode= 0; }
    elsif ($opt eq 'subdir')   { push (@subdirs, $val || shift (@ARGV)); }
    elsif ($opt eq 'cd')       { $cd_mode= 1; }
    elsif ($arg =~ /^--(refresh|verify|lookup|edit|maint|next-seq|get-cat)$/) { $op_mode= $1; }
    else { &usage ("unknown option '$arg'"); }
  }
  elsif ($arg =~ /^-/)
  {
    my @a= split ('|', $arg);
    shift (@a);
    foreach my $a (@a)
    {
         if ($a eq 'p') { $project= shift (@ARGV); }
      elsif ($a eq 's') { $store=   shift (@ARGV); }
      elsif ($a eq 'D') { $DEBUG++; }
      elsif ($a eq 'X') { $STOP= 1; }
      else { &usage ("unknown option '-$a'"); }
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
  exit (2);
}
# &usage ('no store specified') unless (defined ($store));

if ($op_mode eq 'edit')
{
  my ($proj_cfg_dir, $proj_cfg_fnm)= TA::ObjReg::_get_project_paths($project);
  system ($EDITOR, $proj_cfg_fnm);
  # print "store_cfg: ", Dumper ($store_cfg);
  exit(0);
}

my $objreg= new TA::ObjReg ('project' => $project, 'store' => $store, 'key' => 'md5');
# print "objreg: ", Dumper ($objreg); exit;
&usage ('no config found') unless (defined ($objreg));
print "objreg: ", Dumper ($objreg) if ($DEBUG || $STOP);
exit(2) if ($STOP);

$SIG{INT}= sub { $STOP= 1; print "SIGINT received\n"; };

if ($op_mode eq 'refresh')
{
  my $catalog= $objreg->{'cfg'}->{'catalog'};
  &usage ('no catalog found in config') unless (defined ($catalog));

  my $stores_p= $objreg->{'cfg'}->{'stores'};
  my $store_cfg= $stores_p->{$store};
  unless (defined ($store_cfg))
  {
    print "no store config found for '$store', check these: ", Dumper ($stores_p);
    exit (2);
  }

# ZZZ
$DEBUG= 1;
  print "store_cfg: ", Dumper ($store_cfg) if ($DEBUG);
  
  if ($cd_mode == 1)
  {
    my $path= $store_cfg->{'path'} or event_die ("no path defined");
    print "path=[$path]\n";
    my $res= chdir ($path) or event_die ("can not change to $path");;
    print "res=[$res]\n";

    # verify if the chdir really lead to the expected place
    # TODO: there might be symlinked paths or something like that, so this should pssibly not always fail
    my $pwd= `pwd`; chop($pwd);
    print "pwd=[$pwd]\n";
    event_die ("chdir failed strangely path=[$path] pwd=[$pwd]") unless ($pwd eq $path);
  }

  if (exists ($store_cfg->{'inodes'}))
  {
    my $i= $store_cfg->{'inodes'};
       if ($i eq 'ignore') { $check_inode= 0; }
    elsif ($i eq 'check')  { $check_inode= 1; }
    else
    {
      print "WARNING: store-parameter 'inodes' has unknown value '$i'\n";
    }
  }

     if ($catalog->{'format'} eq 'md5cat')   { refresh_md5cat   ($objreg, $store); }
  elsif ($catalog->{'format'} eq 'internal') { refresh_internal ($objreg, $store); }
}
elsif ($op_mode eq 'verify')
{
  $objreg->verify_toc (\&verify_toc_item, \@hdr);
}
elsif ($op_mode eq 'lookup')
{
  foreach my $key (@PAR)
  {
    my $res= $objreg->lookup ( { 'md5' => $key } );
    print "res: ", Dumper ($res);
  }
}
elsif ($op_mode eq 'maint')
{

=begin comment

TODO: For MongoDB backend: synchronize information about stores with maint collection

=end comment
=cut

}
elsif ($op_mode eq 'get-cat')
{
  my $catalog= $objreg->{'cfg'}->{'catalog'};
  &usage ('no catalog found in config') unless (defined ($catalog));

  my $stores_p= $objreg->{'cfg'}->{'stores'};
  my $store_cfg= $stores_p->{$store};
  unless (defined ($store_cfg))
  {
    print "no store config found for '$store', check these: ", Dumper ($stores_p);
    exit (2);
  }
  print "store_cfg: ", Dumper ($store_cfg) if ($DEBUG);

     if ($catalog->{'format'} eq 'md5cat')   { print "hmm... you should have a _catalog already!\n"; }
  elsif ($catalog->{'format'} eq 'internal') { get_cat_internal ($objreg, $store); }
}
elsif ($op_mode eq 'next-seq')
{
  my $x= $objreg->next_seq ();
  print "x: ", Dumper ($x);
}

# print "objreg: (after refresh)", Dumper ($objreg);

if ($STOP)
{
  print "STOP set, exit 2\n";
  exit(2);
}

exit (0);

sub usage
{
  my $msg= shift;
  if ($msg)
  {
    print $msg, "\n";
    sleep (5);
  }
  system ('perldoc', $0);
  exit -1;
}

sub event_die
{
  my $msg= shift;
  # TODO: write to MongoDB if an event collection is known
  print join (' ', caller), "\n";
  print $msg;
  exit (2);
}

sub refresh_md5cat
{
  my $objreg= shift;
  my $store= shift;
  my %extra= @_;

  # my $catalog=  $objreg->{'cfg'}->{'catalog'};
  system ('/usr/local/bin/chkmd5.pl');

  # my $hasher= $objreg->{'hasher'};

  open (CAT, '<:utf8', '_catalog') or event_die "cant read catalog";
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
  printf ("%9d files processed; %9d files updated\n", $cnt_processed, $cnt_updated);
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
  print "TOC verified\n";
  my $toc= $objreg->load_single_toc ($store);
  # print "toc: ", Dumper ($toc);

  my $md5cat= new md5cat ();

  if (@subdirs)
  {
    foreach my $subdir (@subdirs)
    {
      my $subdir_file_list= "find '$subdir' -xdev -type f -print|";
      $md5cat->read_flist ($subdir_file_list);
    }
  }
  else
  {
    $md5cat->read_flist ($DEFAULT_file_list);
  }

  # print "md5cat: ", Dumper ($md5cat);
  print "flist processed\n";

  my @check_list= qw(mtime size);
  push (@check_list, 'ino') if ($check_inode);

  # compare TOC and reference filelist
  my $fl= $md5cat->{'FLIST'};
  my %key= ();
  my $cnt= 0;
  if (defined ($toc))
  {
  # print "toc: ", Dumper ($toc);
  printf ("%9d items to be processed\n", scalar @$toc);
  print "\npass 1\n";
  foreach my $x (@$toc)
  {
    printf ("%9d items processed\n", $cnt) if ((++$cnt % 10000) == 0);
# print __LINE__, " k=[$k]\n";
    my $k= $x->{'key'};
    my $p= $x->{'path'};
    $key{$k}->{$p}= 0;

    if (exists ($fl->{$p}))
    {
      $cnt_processed++;
      my $f= $fl->{$p};
      my $matches= 1;
      AN: foreach my $an (@check_list)
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

# print __LINE__, " check_new_files\n";
  print "\npass 2\n";
  my $new_files= $md5cat->check_new_files ($limit);
  # print "new_files: ", Dumper ($new_files);
  unless ($md5cat->run())
  {
    print "SIGINT received in \$md5cat->check_new_files()\n";
    $STOP= 1;
  }

# print __LINE__, " integrate_md5_sums\n";
  $md5cat->integrate_md5_sums ($new_files);
  # $md5cat->save_catalog (); # TODO: if save_catalog flag is true!

# ZZZ
  # update the Object registry with new items
  printf ("%9d new items to be processed\n", scalar @$new_files);
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
  if (@subdirs)
  {

=begin comment

NOTE: we only inspected a subdirectory, but this inspects everything
and would remove items that were not even inspected

TODO: only drop the thing when it is in the right subdirectory!

=end comment
=cut

    print "NOTE: no check for removable items performed!\n";

  }
  else
  {
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
  }

  printf ("files: %9d processed; %9d updated; %9d (%d) dropped\n",
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
      'mtime' => $st[9], 'fs_size' => $st[7]
    };
    $xdata->{'ino'}= $st[1] if ($check_inode);

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

sub get_cat_internal
{
  my $objreg= shift;
  my $store= shift;

  my $toc= $objreg->load_single_toc ($store);
  # print "toc: ", Dumper ($toc);

  unless (@$toc)
  {
    print "nothing found; exiting\n";
    return undef;
  }

  my $count= 0;
  unless (open (CAT, '>:utf8', $cat_file))
  {
    print "can not write to '$cat_file'\n";
    return undef;
  }
  print "writing new catalog '$cat_file'\n";

  my %inodes;
  foreach my $t (@$toc)
  {
    my ($md5, $fs_size, $path, $ino)= map { $t->{$_} } qw(md5 fs_size path ino);
    printf CAT ("%s file %9ld %s\n", $md5, $fs_size, $path);
    # print "t: ", Dumper ($t);
    push (@{$inodes{$ino}}, $path) if ($check_inode);
    $count++;
  }
  close (CAT);

  if ($check_inode)
  {
    if (open (INO, '>:utf8', $ino_file))
    {
      print "writing new catalog '$ino_file'\n";
      foreach my $ino (sort { $a <=> $b } keys %inodes)
      {
        print INO join ('|', $ino, @{$inodes{$ino}}), "\n";
      }
      close (INO);
    }
    else
    {
      print "can not write to '$ino_file'\n";
    }
  }

  $count;
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

  my @check_list= qw(md5 path mtime fs_size);
  push (@check_list, 'ino') if ($check_inode);
  foreach my $k (@check_list)
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
  * environment variable TABASE:
    * add pod section
    * allow command line option to specify alternative base directory name

=head2 misc

  * maybe it makes sense to offer an option to perform backups along the
    way, for instance, when the store is actually a git repository.
  * also, checking the VCS status (if not committing updates)
    might be useful.
  * other hashing algorithms:
    * currently we use md5 for hashing, however, this code should
      be fairly simple to adopt for sha1, sha256 or something else.
    * possibly, it makes sense to allow several hashing algorithms
      in parallel, however, then it might be a good idea to store
      file metadata in one place and let other hashes point to that
      place.
  * make this code available for the "reposync" project

=head2 embedded databases?

  * maybe embedded MongoDB would be a nice alternative to the full
    blown TCP/IP connected MongoDB backend


