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

my %plugins_loaded= ();

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

  my $be= $proj_cfg->{'backend'};
  unless (exists ($plugins_loaded{$be}))
  {
       if ($be eq 'TA::Hasher')  { require TA::Hasher; }
    elsif ($be eq 'TA::UrxnBla') { require TA::UrxnBla; }
    elsif ($be eq 'MongoDB')     { require MongoDB; }
    else
    {
      print "ATTN: unknown backend '$be'\n";
      return undef;
    }
    $plugins_loaded{$be}= 1;
  }

  if ($be eq 'TA::Hasher')
  {
    # initialize hasher
    my $ta= $proj_cfg->{'TA::Hasher'};
    $ta->{'name'}= 'file';
    $ta->{'pfx'}= $obj->{'proj_cat'}= my $proj_cat= join ('/', $proj_cfg_dir, 'cat');
    $obj->{'hasher'}= my $hasher= new TA::Hasher (%$ta);
  }
  elsif ($be eq 'MongoDB')
  {
    $obj->connect_MongoDB ($proj_cfg);
  }

  # get sequence number
  $obj->{'seq_file'}= my $fnm_seq= join ('/', $proj_cfg_dir, 'sequence.json');
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
  my $search= shift;

  my $be= $obj->{'cfg'}->{'backend'};
  print "lookup [$search] be=[$be]\n";
  print main::Dumper ($search);

  my $reg;
  if ($be eq 'TA::Hasher')
  {
    my $id_str= $search->{$obj->{'key'}};
    my ($all_reg, $fnm)= $obj->ta_retrieve ($id_str, 0);
    print "fnm=[$fnm] all_reg: ", main::Dumper ($all_reg);
    return undef unless (defined ($all_reg));
    ($reg)= ta_match ($all_reg, $search);
  }
  elsif ($be eq 'MongoDB')
  {
    $reg= $obj->{'_col'}->find_one ( $search );
  }
  # print "reg: ", main::Dumper ($reg);
 
  return $reg;
}

sub save
{
  my $obj= shift;
  my $search= shift;
  my $new_reg= shift;

  my $be= $obj->{'cfg'}->{'backend'};
  print "save [$new_reg] be=[$be]\n";
  print main::Dumper ($new_reg);
  if ($be eq 'TA::Hasher')
  {
    my $id_str= $search->{$obj->{'key'}};
    my ($all_reg, $fnm)= $obj->ta_retrieve ($id_str, 1);

=begin comment

    my @st= stat ($fnm);
    unless (@st)
    { # TODO: increment sequence and toc
    }

=end comment
=cut

    if (defined ($all_reg))
    {
      my ($reg, $idx)= ta_match ($all_reg, $search);
      if (defined ($reg))
      {
        $all_reg->{'entries'}->[$idx]= $new_reg;
      }
      else
      {
        push (@{$all_reg->{'entries'}}, $new_reg);
      }
    }
    else
    {
      $all_reg= { 'key' => $id_str, 'entries' => [ $new_reg ] }
    }

    my $j= encode_json ($all_reg);
    print "fnm=[$fnm]\n";
    print "generated json: [$j]\n";
    open (J, '>:utf8', $fnm); print J $j; close (J);
  }
  elsif ($be eq 'MongoDB')
  {
    print "new_reg: ", main::Dumper ($new_reg);
    $obj->{'_col'}->insert ($new_reg);
  }
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
  my $check_item= shift; # callback: update TOC item
  my $hdr= shift || [];

  my @hdr1= qw(seq found store_count);

  my @stores= $reg->stores();
  # print "stores: ", join (', ', @stores), "\n"; exit;

  #### my @extra_fields= (exists ($reg->{'toc_extra_fields'})) ? $reg->{'toc_extra_fields'} : ();
  my $c= $reg->{'proj_cat'};

  # pick up current tocs to see if the sequence needs to be updated
  my %stores;
  foreach my $s (@stores)
  {
    my $f= $c . '/' . $s . '.toc.json';
    my $t= TA::Util::slurp_file ($f, 'json');
    $t= {} unless (defined ($t)); # we need an empty toc if there is no toc yet

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

      my $jj= $j->{'store'}->{$store};
      $ster->{'store_count'}= scalar @i_stores;

      &$check_item($j, $jj, $ster) if (defined ($check_item));
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
    print TOC join (';', 'key', @hdr1, @$hdr), "\n";

    foreach my $k (keys %$ss)
    {
      my $r= $ss->{$k};
      print TOC join (';', $k, map { $r->{$_} } @hdr1, @$hdr), "\n";
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

=head1 INTERNAL METHODS

=head2 $mongo_collection= $obj->connect_MongoDB ($config);

Connect to MongoDB with connection parameters in hash_ref $config and
returns the MongoDB collection object.

$config needs the following attribues: host, db, user, pass, collection

=cut

sub connect_MongoDB
{
  my $obj= shift;
  my $cfg= shift;

  my $cmm= $cfg->{'MongoDB'};
  print "cmm: ", main::Dumper ($cmm);

  my $col;
  eval
  {
    my $connection= MongoDB::Connection->new(host => $cmm->{'host'});
    $connection->authenticate($cmm->{'db'}, $cmm->{'user'}, $cmm->{'pass'});
    my $db= $connection->get_database($cmm->{'db'});
    $col= $db->get_collection($cmm->{'collection'});
    print "col: [$col]\n";
  };
  if ($@)
  {
    print "ATTN: can't connect to MongoDB ", (join ('/', map { $cmm->{$_} } qw(host user collection))), "\n";
    return undef;
  }

  return $obj->{'_col'}= $col;
}

=head2 ($data, $fnm)= $objreg->ta_retrieve ($key, $create)

Retrieve and return data referenced by $key and returns path name of
that file.  If $create is true, the path leading to that file is created,
when it is not already present.

=cut

sub ta_retrieve
{
  my $obj= shift;
  my $id_str= shift;
  my $create= shift;

    my @r= $obj->{'hasher'}->check_file ($id_str, $create);
    # print "id_str=[$id_str] r=", main::Dumper (\@r);
    my ($rc, $path)= @r;

    my $fnm= $path . '/' . $id_str . '.json';
    # print "description: [$fnm]\n";

    my @st= stat ($fnm);
    return (undef, $fnm) unless (@st);

    my $all_reg= TA::Util::slurp_file ($fnm, 'json');

  return ($all_reg, $fnm);
}

=head1 INTERNAL FUNCTIONS

=head2 ($entry, $index)= ta_match ($data, $search)

Select first $entry from $data that matches hash ref $search.

=cut

sub ta_match
{
  my $all_reg= shift;
  my $search= shift;

  my @k= keys $search;
  my @e= @{$all_reg->{'entries'}};
  REG: for (my $i= 0; $i <= $#e; $i++)
  {
    my $reg= $e[$i];
    foreach my $k (@k)
    {
      next REG unless ($reg->{$k} eq $search->{$k});
    }
    print "found match: ", main::Dumper ($reg);
    return ($reg, $i);
  }
  return (undef, 0);
}

1;
__END__

=head1 ENVIRONMENT

=head1 TODOs

* this is a stub for storage in a local filesystem
** connect to centralized connection service


