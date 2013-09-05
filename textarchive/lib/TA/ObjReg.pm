#
# File: lib/TA/ObjReg.pm
#

package TA::ObjReg;

=head1 NAME

  TA::ObjReg  -- Text-Archive Object Registry

=head1 DESCRIPTION

=cut

use strict;

use JSON;
use File::Find;

use TA::Util;
use TA::Hasher;

sub new
{
  my $class= shift;
  my %par= @_;

  # check the presence of all required parameters
  my $stopit= 0;
  foreach my $k (qw(project))
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

=head1 project level methods

=head2 $reg->get_project()

(re)loads the project related data structures

=cut

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
  # TODO: check authorization (no need, if local, but for client-server, we need something

  # initialize hasher
  my $base_dir= $obj->{'proj_cfg_dir'};
  $obj->{'proj_cat'}= my $proj_cat= join ('/', $base_dir, 'cat');
  $obj->{'hasher'}= my $hasher= new TA::Hasher ('algorithm' => $proj_cfg->{'algorithm'}, 'pfx' => $proj_cat, 'name' => 'file');

  # get sequence number
  $obj->{'seq_file'}= my $fnm_seq= join ('/', $base_dir, 'sequence.json');
  $obj->{'seq'}= my $seq= TA::Util::slurp_file ($fnm_seq, 'json');
  # print "seq: ", main::Dumper ($seq);
  unless (defined ($seq))
  {
    $obj->{'seq'}= $seq= { 'seq' => 0, 'upd' => time () };
    $obj->_save_seq ();
  }

  $proj_cfg;
}

=head2 $reg->stores()

returns a list of all stores in the project

=cut

sub stores
{
  my $reg= shift;

  my @stores= keys %{$reg->{'cfg'}->{'stores'}};

  (wantarray) ? @stores : \@stores;
}

=head1 item related methods

=head2 $reg->lookup($key)

returns that keys value, if present, otherwise, undef.

=cut

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

  my @st= stat ($fnm);
  unless (@st)
  { # TODO: increment sequence and toc
  }

  my $j= encode_json ($new_reg);
  # print "generated json: [$j]\n";
  open (J, '>:utf8', $fnm); print J $j; close (J);
}

=head1 TOC: Table of Contents

single TOC format:
   key:
   {
     "seq": number, # this items sequence number
     "upd": epoch   # 
   }

global TOC format:
   key:
   {
     "seq": number,
     "stores": [ { store-id: ..., "upd": epoch } ]
   }

The toc file is stored in:

  <project>/cat/<store-id>.toc.json

=head2 $reg->load_toc ($store)

returns toc hashed by key.

if $store is undef, returns a toc of all stores

=cut

sub load_toc_v1
{
  my $reg= shift;
  my $store= shift;
  my $cache= shift;

  my $c= $reg->{'proj_cat'};
  return undef unless (defined ($c)); # not initialized?

  my @stores= (defined ($store)) ? $store : $reg->stores();

  return undef unless (@stores); # return nothing if there is nothing...

  my $toc= {};
  foreach my $s (@stores)
  {
    my $f= $c . '/' . $s . '.toc.json';
    my $t= TA::Util::slurp_file ($f, 'json');
    if ($cache)
    {
      $reg->{'tocs'}->{$s}= $t;
    }

    foreach my $k (keys %$t)
    {
      my $r;

      unless (defined ($r= $toc->{$k}))
      { # not yet present in the toc
        $toc->{$k}= $r= { 'sequence' => $k->{'sequence'} };
      }

      push (@{$r->{'stores'}}, { 'store' => $s, 'upd' => $k->{'upd'} });
    }
  }

  $toc;
}

sub verify_toc
{
  my $reg= shift;

print "sub verify_toc_v1\n";
  # my $store= shift; this does not make sense, we need to verify verything anyway

  # my @stores= (defined ($store)) ? $store : $reg->stores();
  my @stores= $reg->stores();
  # print "stores: ", join (', ', @stores), "\n"; exit;
  my %stores;

  my @extra_fields= (exists ($reg->{'toc_extra_fields'})) ? $reg->{'toc_extra_fields'} : ();

  # TODO: this is specific for vlib001.pl, this should be a passed as code ref!
  my @hdr= qw(seq found store_count path_count path mtime fs_size ino);

  my $c= $reg->{'proj_cat'};
  # pick up current tocs to see if the sequence needs to be updated
  foreach my $s (@stores)
  {
    my $f= $c . '/' . $s . '.toc.json';
    my $t= TA::Util::slurp_file ($f, 'json');
    $t= {} unless (defined ($t)); # we need an empty toc if there is none yet

    $stores{$s}= $t;
  }

  my %items;
  sub item_files
  {
    next if ($_ =~ /\.toc\.json$/);
    next if ($_ =~ /\.toc\.csv$/);
    next unless ($_ =~ /\.json$/ && -f (my $x= $File::Find::name));

    # print "file=[$_] path=[$x]\n";

    $items{$_}= [ $x ];
  }

  my $d= $reg->{'proj_cat'};
  print "proj_cat=[$d]\n";
  find (\&item_files, $d);

  # print "items: ", main::Dumper (\%items);
  foreach my $item (keys %items)
  {
    my $p= $items{$item};
    my $j= TA::Util::slurp_file ($p->[0], 'json');
    # print "[$p->[0]] j: ", main::Dumper ($j);
    my @i_stores= keys %{$j->{'store'}};
    my $key= $j->{'key'};
    # print join (' ', $key, @i_stores), "\n";

    # search for a key's sequence number in all known stores, not only
    # in those that are *currently* used for this store
    my $seq;
    S1: foreach my $store (@stores)
    {
      if (exists ($stores{$store}->{$key}))
      {
        $seq= $stores{$store}->{$key}->{'seq'};
        last S1;
      }
    }

    S2: foreach my $store (@i_stores)
    {
      my $ster; # store's toc entry record ;)
      unless (defined ($ster= $stores{$store}->{$key}))
      {
        $ster= $stores{$store}->{$key}=
        {
          'seq' => $reg->next_seq(),
          'upd' =>  time (),
        };
      }
      $ster->{'found'}= 1;

      # TODO: this is specific for vlib001.pl, this should be a passed as code ref!
      my $jj= $j->{'store'}->{$store};
      my @paths= keys %{$jj->{'path'}};
      $ster->{'path_count'}= scalar @paths;
      $ster->{'store_count'}= scalar @i_stores;
      my $p1= shift (@paths);
      my $px1= $jj->{'path'}->{$p1};

      $ster->{'path'}= $p1;
      $ster->{'mtime'}= $px1->{'mtime'};
      $ster->{'fs_size'}= $px1->{'fs_size'};
      $ster->{'ino'}= $px1->{'ino'};
    }
  }

  print "finishing\n";
  # save all tocs now
  foreach my $s (@stores)
  {
    my $ss= $stores{$s};

    my $f= $c . '/' . $s . '.toc.json';
    print "saving toc to [$f]\n";
    unless (open (TOC, '>:utf8', $f))
    {
      print STDERR "cant save toc file '$f'\n";
      next;
    }
    print TOC encode_json ($ss), "\n";
    close (TOC);

    $f= $c . '/' . $s . '.toc.csv';
    print "saving toc to [$f]\n";
    unless (open (TOC, '>:utf8', $f))
    {
      print STDERR "cant save toc file '$f'\n";
      next;
    }
    print TOC join (';', 'key', @hdr), "\n";

    foreach my $k (keys %$ss)
    {
      my $r= $ss->{$k};
      # TODO: this is specific for vlib001.pl, this should be a passed as code ref!
      print TOC join (';', $k, map { $r->{$_} } @hdr), "\n";
    }

    close (TOC);
  }

  # TODO: return something meaningful
}

=head1 sequence number

=head2 $reg->next_seq()

=cut

sub flush
{
  my $reg= shift;

  $reg->_save_seq ();
}

sub _save_seq
{
  my $reg= shift;

  my $f= $reg->{'seq_file'};
  open (F_SEQ, '>:utf8', $f) or die "cant write sequence to '$f'";
  print F_SEQ encode_json ($reg->{'seq'}), "\n";
  close (F_SEQ);
}

sub next_seq
{
  my $reg= shift;

  my $seq= $reg->{'seq'};
  $seq->{'seq'}++;
  $seq->{'upd'}= time ();
  $reg->_save_seq (); # TODO: optionally delay that until $reg->flush();

  $seq->{'seq'};
}

# =head1 INTERNAL FUNCTIONS

1;
__END__

=head1 ENVIRONMENT

=head1 TODOs

* this is a stub for storage in a local filesystem
** connect to centralized connection service


