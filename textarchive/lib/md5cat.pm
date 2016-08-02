#
# $Id: md5cat.pm,v 1.9 2016/08/02 08:24:55 gonter Exp $
#

=head1 NAME

  md5cat

=head1 DESCRIPTION

=head1 SYNOPSIS

  my $md5cat= new md5cat (%parameters);

=head1 REQUIREMENTS

  Digest::MD5::File
  sudo apt-get install libdigest-md5-file-perl

=cut

package md5cat;

use strict;

use Digest::MD5::File;

my $run= 1;
sub run { $run; }

sub new
{
  my $class= shift;
  my %par= @_;

  my $md5cat=
  {
    'catalog' => '_catalog',
    'format' => 'md5cat', # standard catalog format; alternative: md5sum
    'chksum_pgm' => 'Digest::MD5::File',
  };
  bless $md5cat, $class;
  foreach my $par (keys %par)
  {
    $md5cat->{$par}= $par{$par};
  }
  $md5cat->{'FLIST'}= {};
  $md5cat->{'INO'}= {};

  $md5cat->set_catalog ();

  $SIG{INT}= sub { $run= 0 };

  $md5cat;
}

sub set_catalog
{
  my $obj= shift;
  my $catalog= shift || $obj->{'catalog'};

  $obj->{'cat_backup'}=    $catalog . '.bup';
  $obj->{'inocat_file'}=   $catalog . '.inodes';
  $obj->{'inocat_backup'}= $catalog . '.inodes.bup';
}

sub set_tmpdir
{
  my $obj= shift;
  my $tmp_dir= shift;

  $obj->{'tmp_out'}=    $tmp_dir . '@@@md5.tmp';
}

# ----------------------------------------------------------------------------
sub save_catalog
{
  my $md5cat= shift;
  my $cat= shift || $md5cat->{'catalog'};

  my $bup= $cat . '.bup';

  rename $cat, $bup if (-r $cat);
  unless (open (CAT, ">$cat"))
  {
    print "can't write to [$cat]\n";
    return -1;
  }
  print "writing new catalog '$cat'\n";

  my $xFLIST= $md5cat->{'FLIST'};
  foreach my $xf (sort keys %$xFLIST)
  {
    my $xfo= $xFLIST->{$xf};
    printf CAT ("%s file %9d %s\n", $xfo->{'md5'}, $xfo->{'fs_size'}, $xf);
    # print CAT $CAT{$entry}, "\n";
  }
  close (CAT);
}

# ----------------------------------------------------------------------------
sub save_inode_catalog
{
  my $md5cat= shift;
  my $cat= shift || $md5cat->{'inocat_file'};

  my $bup= $cat . '.bup';

  rename $cat, $bup if (-r $cat);
  unless (open (CAT, ">$cat"))
  {
    print "cant write to $cat\n";
    return -1;
  }
  print "writing new catalog '$cat'\n";

  my $INO= $md5cat->{'INO'};
  my $entry;
  foreach $entry (sort { $a <=> $b } keys %$INO)
  {
    print CAT join ('|', $entry, @{$INO->{$entry}}), "\n";
  }
  close (CAT);
}

# ----------------------------------------------------------------------------
sub read_flist
{
  my $md5cat= shift;
  my $fnm= shift;

  my $xFLIST= $md5cat->{'FLIST'};
  my $INO= $md5cat->{'INO'};

  print "reading reference list: [$fnm]\n";
  unless (open (FI, $fnm))
  {
    print __LINE__, " could not open '$fnm'!\n";
    return -1;
  }

  my $ref_cnt= 0;
  while (<FI>)
  {
    s/\015//g;
    chop;
    next if (/^#/);
    tr/A-Z/a-z/ if ($md5cat->{'to_lower'});
    s#^\.\/##;

    # print __LINE__, " >>> $_\n" if (/Hafo/i);
    # print ">>> $_\n";

# TODO: refactor into separate check
    next if ($_ eq $md5cat->{'catalog'}
             || $_ eq $md5cat->{'cat_backup'}
             || $_ eq $md5cat->{'inocat_file'}
             || $_ eq $md5cat->{'inocat_backup'}
            );
    next if ($md5cat->{'skip_vcs'} && $_ =~ m#(^|/)(CVS|\.git|\.svn|.bzr|RCS)/#);

    my @st= stat ($_);
    my $ino= $st[1];

    $xFLIST->{$_}= { 'state' => 'exists', 'fs_size' => $st[7], 'mtime' => $st [9], 'ino' => $ino };
    $ref_cnt++;

    push (@{$INO->{$ino}}, $_);
  }
  close (FI);
  $ref_cnt;
}

# ----------------------------------------------------------------------------
sub read_dir_listing
{
  my $md5cat= shift;
  my $fnm_i= shift;
  my $is_top;
  my $dir= '';
  my ($perms, $lnk, $owner, $group, $size, $mon, $day, $time_year, $nm);

  my $xFLIST= $md5cat->{'FLIST'};

  print "reading dir listing: '$fnm_i'\n";
  unless (open (FI, $fnm_i))
  {
    print __LINE__, " could not open '$fnm_i'!\n";
    return -1;
  }

  my $ref_cnt= 0;
  while (<FI>)
  {
    s/\015//g;
    chop;

    if (/^[ \t]*$/)
    {
      $is_top= 1;
      next;
    }

    next if (/^total /);

    if ($is_top && /\.\/pub\/(.+):/)
    {
      $dir= $1;
      $is_top= 0;
      next;
    }

    ($perms, $lnk, $owner, $group, $size, $mon, $day, $time_year, $nm)=
      split (' ', $_, 9);

    if ($perms =~ /^\-r..r..r../)
    {
      $size= 1 if ($size == 0); # 0 is a flag !
      $xFLIST->{"$dir/$nm"}= { 'fs_size' => $size, 'state' => 'exists' };
      $ref_cnt++;
    }
  }
  close (FI);
  $ref_cnt;
}

# ----------------------------------------------------------------------------
# cleanup
sub check_new_files
{
  my $md5cat= shift;
  my $limit= shift;

  my %QUEUE_openssl= ();
  my $QUEUE_openssl= 0;

  my $xFLIST= $md5cat->{'FLIST'};
  my @tmp_2chk= ();
  my @res= ();
  my $cnt= 0;
  FILE: foreach my $fnm (sort keys %$xFLIST)
  {
    last FILE if (defined ($limit) && @tmp_2chk >= $limit);

    printf ("%9d items processed\n", $cnt) if ((++$cnt % 10000) == 0);
    my $F= $xFLIST->{$fnm};
    my $x= $F->{'state'};
    next if ($x eq 'nocheck');

    if ($x eq 'toobig')
    {
      print "ATTN: skipping '$fnm': '$x'\n";
      next;
    }

    # print "to be checked: fnm='$fnm'\n" if ($md5cat->{'verbose_level'} > 2);

    if (0 && $F->{'fs_size'} >= $md5cat->{'MAX_CSIZE'})
    {
      print "file to big for checksum!\n";
      print 'F: ', main::Dumper ($F), "\n";
      $QUEUE_openssl{$fnm}= $F;
      $QUEUE_openssl++;
    }
    else
    {
      push (@tmp_2chk, $fnm);
    }
  }
  printf ("%9d files to be checked\n", scalar (@tmp_2chk));

  if (@tmp_2chk)
  {
    print "to be checked with $md5cat->{'chksum_pgm'}: ", scalar @tmp_2chk, "\n" if ($md5cat->{'verbose_level'} > 2);
    # print __LINE__, " files:\n--- 8< ---\n", join ("\n", @tmp_2chk), "\n--- >8 ---\n";

    if ($md5cat->{'chksum_pgm'} eq 'Digest::MD5::File')
    {
      @res= digest_md5_list (@tmp_2chk);
      # print "res: ", main::Dumper (\@res);
    }

=begin comment

... maybe we should drop that sometime ...
    else
    {
    my $cmd;
    if ($md5cat->{'chksum_pgm'} eq 'checksum')
    {
      $cmd= "$md5cat->{'checksum'} -e -f$tmp_2chk >$md5cat->{'tmp_out'}";
    }
    elsif ($md5cat->{'chksum_pgm'} eq 'CRC32')
    {
      $cmd= "CRC32 -checksum -f$tmp_2chk >$md5cat->{'tmp_out'}";
    }
    elsif ($md5cat->{'chksum_pgm'} eq 'md5sum')
    {
      $cmd= "xargs <$tmp_2chk $md5cat->{'checksum'} >$md5cat->{'tmp_out'}";
    }
    if ($md5cat->{'chksum_pgm'} eq 'Digest::MD5::File')
    {
      digest_md5_file ($tmp_2chk, $md5cat->{'tmp_out'});
    }

    print "chksum_pgm=[$md5cat->{'chksum_pgm'}]\n";
    print "checksum=[$md5cat->{'checksum'}]\n";
    system ($cmd);
    # exit (0);
    }

=end comment
=cut

  }

    if ($QUEUE_openssl)
    {
      print "to be checked with openssl: $QUEUE_openssl\n" if ($md5cat->{'verbose_level'} > 2);
      open (FO, ">>$md5cat->{'tmp_out'}") or die "cant append to $md5cat->{'tmp_out'}";

      print "to be checked with openssl: $QUEUE_openssl\n";
      foreach my $fnm (keys %QUEUE_openssl)
      {
        my $F= $QUEUE_openssl{$fnm};
        my $cmd2= "openssl md5 '$fnm'";
	my $res= `$cmd2`;
	chop ($res);

	print __LINE__, " >> res='$res'\n";
	if ($res =~ /^MD5\((.+)\)= ([a-fA-F0-9]{32,})$/)
	{
	  my ($x_fnm, $x_md5)= ($1, $2);
	  print __LINE__, " >>> x_fnm='$x_fnm' x_md5='$x_md5'\n";
	  print FO "$x_md5 file $F->{'fs_size'} $x_fnm\n";
	}
      }
      close (FO);
    }

  push (@main::REPORT, sprintf ("checked %s: %8d (%s)", $md5cat->{'chksum_pgm'}, scalar (@tmp_2chk)));
  push (@main::REPORT, sprintf ("checked openssl: %8d (%s)", $QUEUE_openssl));

  (wantarray) ? @res : \@res;
}

sub update_catalog
{
  my $md5cat= shift;

  # Step 2: read catalog of known files
  my $to_save= $md5cat->check_md5_entries ($md5cat->{'catalog'}, 0);
  # print __LINE__, " diag: catalog processed\n"; $md5cat->diag ();
  # print __LINE__, " md5cat: ", main::Dumper ($md5cat);

  my $new_files= $md5cat->check_new_files ();
  if (@$new_files)
  {
    # $md5cat->check_md5_entries ($md5cat->{'tmp_out'}, 1);
    # unlink $md5cat->{'tmp_out'} if ($md5cat->{'do_unlink'});
    print __LINE__, " integrate new files: ", main::Dumper ($new_files);
    $md5cat->integrate_md5_sums ($new_files);

    $to_save= 1;
  }

  if ($to_save)
  {
    $md5cat->save_catalog ();
    $md5cat->save_inode_catalog ();
  }

  $to_save;
}

sub integrate_md5_sums
{
  my $md5cat= shift;
  my $result_list= shift;

  my $fl= $md5cat->{'FLIST'};
  foreach my $e (@$result_list)
  {
    my ($md5, $path, $size, $mtime)= @$e;
    my $f;
    $f= $fl->{$path}= {} unless (defined ($f= $fl->{$path}));
    $f->{'md5'}= $md5;
    $f->{'fs_size'}= $size if (defined ($size));
    $f->{'mtime'}= $mtime if (defined ($mtime));
  }
}

# ----------------------------------------------------------------------------
# read MD5 catalog
#
# this function is called to read the existing catalog as well to read
# output from checksum command
#
sub check_md5_entries
{
  my $md5cat= shift;
  my $list= shift;
  my $added= shift;
  my $format= $md5cat->{'format'} || 'md5cat';

  my $xFLIST= $md5cat->{'FLIST'};

  my $rc= 0;
  my ($cnt_dropped, $cnt_retained, $fnm1, $fnm2, $fnm3, @fnm);
  my @st= stat ($list);
  my $mtime= $st[9];

  unless (open (FI, $list))
  {
    print __LINE__, " could not open $list!\n";
    return 0;
  }

  my $line= 0;
  while (<FI>)
  {
    $line++;
    s/\015//g;
    chop;
    next if (/^\s*$/ || /^#/);

    my ($md5, $x_file, $size, $fnm)= split (' ', $_, 4);

    if ($x_file eq 'file') {} # standard format
    elsif ($fnm eq 'file')
    {
      print STDERR "ATTN: non standard format in file $list at line $line: [$_]\n";
    }
    elsif ($format eq 'md5sum')
    {
      # Uh, this needs to be redesigned soon!
      my @stf= stat ($fnm);
      $size= $stf[7];
      # $_= sprintf ("%s file %9d %s", $f[0], $size, $fnm);
    }
    else
    {
      next;
    }

=begin comment

this may be obsolete and shold be removed
    if (0 && $xsep)  # redesign!
    {
      @fnm= split ('/', $fnm);
      $fnm3= pop (@fnm);
      $fnm2= pop (@fnm);
      $fnm1= substr ($fnm3, 0, 1);
      # print ">> $fnm, $fnm2, $fnm1, $fnm3 ->";

      if ($fnm1 =~ /\d/) { $fnm1= '0'; }
      elsif (!($fnm1 =~ /\w/)) { $fnm1= '_'; }

      if ($fnm2 eq '.xvpics') { push (@fnm, $fnm1, $fnm2, $fnm3); }
      else { push (@fnm, $fnm2, $fnm1, $fnm3); }
      $fnm= join ('/', @fnm);
      # print " $fnm\n";
    }

=end comment
=cut

    my $FL;
    if (# -r $fnm
        defined ($FL= $xFLIST->{$fnm})
        && $FL->{'state'} eq 'exists'
        && !($fnm eq $md5cat->{'catalog'} || $fnm eq $md5cat->{'cat_backup'})
       )
    {
      my $fs_size= $FL->{'fs_size'};
      $FL->{'md5'}= $md5;

      # print "size=$size fs_size=$fs_size\n";
      if (($fs_size == 1 || $size == $fs_size)
          && $mtime > $FL->{'mtime'}
         )
      {
        $cnt_retained++;
        # $FL->{'skip'}= 1;
        $FL->{'state'}= ($added) ? 'added' : 'nocheck';
        # $xFLIST->{$fnm}= 0 if (defined ($xFLIST->{$fnm}) && $xFLIST->{$fnm});
        # $CAT{$fnm}= $_;
      }
      elsif ($fs_size < 0) ####  || $fs_size > $md5cat->{'MAX_CSIZE'})
      { # checksum can't handle files larger than 2 GB yet, it will check such files everytime again
        print ">>> skipping fnm='$fnm' fs_size='$fs_size'\n";
        $FL->{'state'}= 'toobig';
      }
      else
      {
        $FL->{'state'}= ($added) ? 'added' : 'recheck';
      }
      # print "$_\n";
    }
    else
    {
      print "dropped: $_\n";
      $cnt_dropped++;
      $rc= 1;
      $FL->{'state'} => 'dropped';
    }
  }

  close (FI);
  push (@main::REPORT, sprintf ("retained: %8d (%s)", $cnt_retained, $list));
  push (@main::REPORT, sprintf ("dropped:  %8d (%s)", $cnt_dropped,  $list));

  return $rc;
}

# =head1 INTERNAL METHODS

sub diag
{
  my $md5cat= shift;

  my $xF= $md5cat->{'FLIST'} or die;

  foreach my $xf (keys %$xF)
  {
    my $xfo= $xF->{$xf};
    next if ($xfo->{'state'} eq 'nocheck');
    print "xf=[$xf] ", main::Dumper ($xfo);
  }
}

# =head1 INTERNAL FUNCTIONS

sub digest_md5_list
{
  my @res= ();

  my $cnt= 0;
  MD5: while (my $f= shift (@_))
  {
    printf ("%9d items processed\n", $cnt) if ((++$cnt % 10000) == 0);

    last MD5 unless ($run);

    my @st= stat ($f);
    unless (@st)
    {
      print STDERR "can't stat file '$f'\n";
      next;
    }

    my $md5= Digest::MD5::File::file_md5_hex ($f);
printf ("%9d %s %s\n", $st[7], $md5, $f);
    push (@res, [ $md5, $f, $st[7], $st[9] ]);
  }

  (wantarray) ? @res : \@res;
}

sub digest_md5_file
{
  my $fnm_in= shift;
  my $fnm_out= shift;

  open (FI, '<:utf8', $fnm_in)  or die "can't read $fnm_in";
  open (FO, '>:utf8', $fnm_out) or die "can't write $fnm_out";
  while (my $f= <FI>)
  {
    chop ($f);

    my @st= stat ($f);
    unless (@st)
    {
      print STDERR "can't stat file '$f'\n";
      next;
    }

    my $md5= Digest::MD5::File::file_md5_hex ($f);
    printf FO ("%s file %9d %s\n", $md5, $st[7], $f);
  }
  close (FI);
  close (FO);
}

1;

__END__

=head1 INTERNALS

=head2 file items:

state:
  exists  .. file is present in reference list
  nocheck .. no checking necessary (md5 present, mtime ok, size ok)
  recheck .. perform a new check
  toobig  .. file is too big to be handled by normal checking code
  dropped .. item will be removed from the catalog
  added   .. file is added to the catalog


