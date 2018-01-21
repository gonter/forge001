#
# File: lib/TA/ObjReg.pm
#

package TA::ObjReg;

=head1 NAME

  TA::ObjReg  -- Text-Archive Object Registry

=head1 DESCRIPTION

=head1 SYNOPSIS

=cut

use strict;

# use JSON; not used here??
use File::Find;

use TA::Util;
use Util::MongoDB;

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
  $obj->{'cfg'}= $cfg;

  $obj;
}

=head1 PROJECT LEVEL METHODS

=head2 $reg->get_project()

(re)loads the project related data structures

=cut

sub _get_project_paths
{
  my $proj_name= shift;

  my $proj_cfg_dir;
  if (exists ($ENV{'TAPROJ'}))
  {
    $proj_cfg_dir = join ('/', $ENV{'TAPROJ'}, $proj_name);
  }
  else
  {
    $proj_cfg_dir = join ('/', $ENV{'TABASE'}, 'projects', $proj_name);
  }
  my $proj_cfg_fnm= join ('/', $proj_cfg_dir, 'config.json');

  return ($proj_cfg_dir, $proj_cfg_fnm);
}

sub get_project
{
  my $obj= shift;

  my $proj_name= $obj->{'project'};
  my ($proj_cfg_dir, $proj_cfg_fnm)= _get_project_paths ($proj_name);

  $obj->{'proj_cfg_dir'}= $proj_cfg_dir;
  $obj->{'proj_cfg_fnm'}= $proj_cfg_fnm;

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
    elsif ($be eq 'MongoDB')
    {
      require MongoDB;
      # will be included by module "MongoDB" : require MongoDB::MongoClient;
    }
    else
    {
      print "ATTN: unknown backend '$be'\n";
      return undef;
    }
    $plugins_loaded{$be}= 1;
  }

  my $seq;
  if ($be eq 'TA::Hasher')
  {
    # initialize hasher
    my $ta= $proj_cfg->{'TA::Hasher'};
    $ta->{'name'}= 'file';
    $ta->{'pfx'}= $obj->{'proj_cat'}= my $proj_cat= join ('/', $proj_cfg_dir, 'cat');
    $obj->{'hasher'}= my $hasher= new TA::Hasher (%$ta);

    # get sequence number
    $obj->{'seq_file'}= my $fnm_seq= join ('/', $proj_cfg_dir, 'sequence.json');
    $obj->{'seq'}= $seq= TA::Util::slurp_file ($fnm_seq, 'json');

  }
  elsif ($be eq 'MongoDB')
  {
    if ($obj->connect_MongoDB ($proj_cfg))
    {
      my $x= $obj->{'_maint'}->find_one ( { 'an' => 'seq' } );
      $obj->{'seq'}= $seq= $x->{'av'};
    }
    else
    {
      return undef;
    }
  }

  # print "seq: [$seq] ", main::Dumper ($seq);
  unless (defined ($seq))
  {
    $obj->{'seq'}= $seq= { 'seq' => 0, 'upd' => time () };
print "new seq: ", main::Dumper ($seq);
#   if ($be eq 'MongoDB')
#   {
#     $obj->{'_maint'}->insert ( { 'an' => 'seq', 'av' => $seq } );
#   }
#   else
#   {
      $obj->_save_seq ();
#   }
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
  # print "lookup [$search] be=[$be] ", join (' ', %$search), "\n";
  # print main::Dumper ($search);

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
    $reg= $obj->{'_cat'}->find_one ( $search );
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
  # print "save [$new_reg] be=[$be]\n";
  # print main::Dumper ($new_reg);

  my $id_str= $search->{my $key_attr= $obj->{'key'}};
  if ($be eq 'TA::Hasher')
  {
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
      $all_reg= { 'key' => $id_str, 'type' => $key_attr, 'entries' => [ $new_reg ] }
    }

    my $j= encode_json ($all_reg);
    # print "fnm=[$fnm]\n";
    # print "generated json: [$j]\n";
    open (J, '>:utf8', $fnm); print J $j; close (J);
  }
  elsif ($be eq 'MongoDB')
  {
    unless (exists ($new_reg->{'seq'}))
    { # no sequence number known, check if there is one already for that key
      $new_reg->{'seq'}= $obj->mdb_get_seq_for_key ($id_str);
    }

    unless (exists ($new_reg->{'key'}))
    {
      $new_reg->{'key'}= $id_str;
      $new_reg->{'type'}= $key_attr;
    }

    # print "new_reg: ", main::Dumper ($new_reg);
    $obj->{'_cat'}->update ($search, $new_reg, { 'upsert' => 1 } );
  }
}

sub mdb_get_seq_for_key
{
  my $obj= shift;
  my $id_str= shift;

  my $s= { 'key' => $id_str };
  my $k= $obj->{'_keys'};
  my $kv= $k->find_one ($s);

  return $kv->{'seq'} if (defined ($kv));

  $s->{'seq'}= my $seq= $obj->next_seq ();
  $k->insert ($s);
  $seq;
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

sub load_single_toc
{
  my $reg= shift;
  my $store= shift;
  my $cache= shift;

print "load_single_toc: store=[$store]\n";
  if ((my $be= $reg->{'cfg'}->{'backend'}) eq 'TA::Hasher')
  {
    my $c= $reg->{'proj_cat'};
    return undef unless (defined ($c)); # not initialized?

    my $f= $c . '/' . $store . '.toc.json';
    my $t= TA::Util::slurp_file ($f, 'json');
    if ($cache)
    {
      $reg->{'tocs'}->{$store}= $t;
    }
    return $t;
  }
  elsif ($be eq 'MongoDB')
  {
    my $cursor= $reg->{'_cat'}->find ( { 'store' => $store } );
    # print "cursor=[$cursor]\n";
    my @all= $cursor->all ();
    return \@all;
  }
  else
  {
    print "ATTN: load_single_toc not defined for backend '$be'\n";
  }
  return undef;
}

sub load_multi_tocs
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
      my $tk= $t->{$k};
# print "k=[$k] item: ", main::Dumper ($tk);
      my $r;

      unless (defined ($r= $toc->{$k}))
      { # not yet present in the toc
        $toc->{$k}= $r= { 'seq' => $t->{$k}->{'seq'} };
      }

# print "r: ", main::Dumper ($r);

      push (@{$r->{'stores'}}, { 'store' => $s, 'upd' => $tk->{'upd'} });
    }
  }

  $toc;
}

sub verify_toc
{
  my $reg= shift;
  my $check_item= shift; # callback: update TOC item
  my $hdr= shift || [];
  my $reset= shift;

  unless ((my $be= $reg->{'cfg'}->{'backend'}) eq 'TA::Hasher')
  {
    print "ATTN: verify_toc not defined for backend '$be'\n";
    return undef;
  }

  my @hdr1= qw(key seq found store_count);
  # my @hdr1= qw(seq store_count);

  my @stores= $reg->stores();
  # print "stores: ", join (', ', @stores), "\n"; exit;

  #### my @extra_fields= (exists ($reg->{'toc_extra_fields'})) ? $reg->{'toc_extra_fields'} : ();
  my $c= $reg->{'proj_cat'};
  unless (defined ($c))
  {
    print "ERROR: verify_toc no proj_cat directory defined\n";
    return undef;
  }

  # get key to sequence mapping
  my $fnm_key_seq= $reg->{'proj_cfg_dir'} . '/KEY-SEQ.json';
  my $KEY_SEQ;
  $KEY_SEQ= TA::Util::slurp_file ($fnm_key_seq, 'json') unless ($reset);
  $KEY_SEQ= {} unless (defined $KEY_SEQ);

  # pick up current tocs to see if the sequence needs to be updated
  my %stores;
  foreach my $s (@stores)
  {

=begin comment

    my $f= $c . '/' . $s . '.toc.json';
    my $t;
    $t= TA::Util::slurp_file ($f, 'json') unless ($reset);
    if (defined ($t))
    {
      foreach my $e (@$t) { $e->{'found'}= 0; }
    }
    else
    {
      $t= []; # we need an empty toc if there is no toc yet
    }
    $stores{$s}= $t;

  ... dunno ... do we need the old toc?

=end comment
=cut

    $stores{$s}= [];
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

  print __LINE__, " proj_cat=[$c]\n";
  find (\&item_files, $c);

  my $key_seq_updated= 0;
  # print "items: ", main::Dumper (\%items);
  foreach my $item (keys %items)
  {
    my $p= $items{$item};
    my $j= TA::Util::slurp_file ($p->[0], 'json');
    # print "[$p->[0]] j: ", main::Dumper ($j);

    my $key= $j->{'key'};
    my $seq= $KEY_SEQ->{$key};
    unless (defined ($seq))
    {
      $seq= $KEY_SEQ->{$key}= $reg->next_seq();
      $key_seq_updated++;
    }

    # search for a key's sequence number in all known stores, not only
    # in those that are *currently* used for this store
    my (@i_stores, %i_stores);
    E1: foreach my $jj (@{$j->{'entries'}})
    {
      my $store= $jj->{'store'};

      # print join ('/', $key, $seq, $store), "\n";

      $i_stores{$store}= $jj;
      push (@i_stores, $store);
 
      my $ster=
      {
        'key' => $key,
        'seq' => $seq,
        'found' => 0, # flag that indicates if object is present (not used here?)
        'upd' =>  time (),
      };

      &$check_item($j, $jj, $ster) if (defined ($check_item));
      # print "ster: ", main::Dumper ($ster);
      push (@{$stores{$store}}, $ster);
    }
  }

  print "finishing\n";
  # save all tocs now
  foreach my $s (@stores)
  {
    my $ss= $stores{$s};

    # save TOC in json format
    my $f= $c . '/' . $s . '.toc.json';
    print "saving toc to [$f]\n";
    unless (open (TOC, '>:utf8', $f))
    {
      print STDERR "cant save toc file '$f'\n";
      next;
    }
    print TOC encode_json ($ss), "\n";
    close (TOC);

    # save TOC in CSV format
    $f= $c . '/' . $s . '.toc.csv';
    print "saving toc to [$f]\n";
    unless (open (TOC, '>:utf8', $f))
    {
      print STDERR "cant save toc file '$f'\n";
      next;
    }
    print TOC join (';', @hdr1, @$hdr), "\n";

    foreach my $r (@$ss)
    {
      # print __LINE__, " r: ", main::Dumper ($r);
      print TOC join (';', map { $r->{$_} } @hdr1), ';';

      if (1 || $r->{'found'})
      {
        print TOC join (';', map { $r->{$_} } @$hdr);
      }
      else
      {
        print TOC join (';', map { '' } @$hdr);
      }
        
      print TOC "\n";
    }
    close (TOC);
  }

  if ($key_seq_updated)
  {
    print "saving toc to [$fnm_key_seq]\n";
    unless (open (KEY_SEQ, '>:utf8', $fnm_key_seq))
    {
      print STDERR "cant save toc file '$fnm_key_seq'\n";
      next;
    }
    print KEY_SEQ encode_json ($KEY_SEQ), "\n";
    close (KEY_SEQ);
  }

  # TODO: return something meaningful
}

sub remove_from_store
{
  my $objreg= shift;
  my $store= shift;
  my $drop_list= shift; # array ref containing entries: [ $md5, $path ]
  # TODO: maybe a more universal format could be useful

  my $be= $objreg->{'cfg'}->{'backend'};
  if ($be eq 'TA::Hasher')
  {
    my %drop;
    foreach my $item (@$drop_list)
    {
      my ($id_str, $path)= @$item;
      my ($new_rec, $removed)= $objreg->ta_remove ($id_str, { 'store' => $store, 'path' => $path } );
      $drop{$id_str}= $removed if (@$removed);
    }
    return \%drop;
  }
  elsif ($be eq 'MongoDB')
  {
    my $_cat= $objreg->{'_cat'};
    foreach my $item (@$drop_list)
    {
      my ($id_str, $path)= @$item;
      print "drop: key=[$id_str] store=[$store] path=[$path]\n";
      $_cat->remove ( { 'key' => $id_str, 'type' => $objreg->{'key'},
         'store' => $store, 'path' => $path } );
    }
    return {}; # TODO: TA::Hasher variant returns dropped items
  }
}

=begin comment
=head2 $objreg->remove_store ($store);

remove all items that belong to a certain store

sub remove_store
{
  my $reg= shift;
  my $store= shift;
  # TODO: again, maybe a more universial format could be useful

  my $be= $reg->{'cfg'}->{'backend'};
  if ($be eq 'TA::Hasher')
  {
    my %drop;
    # TODO: need a function that returns all the items (that belong to that store)
    foreach my $item (@$drop_list)
    {
      my ($id_str, $path)= @$item;
      my ($new_rec, $removed)= $objreg->ta_remove ($id_str, { 'store' => $store, 'path' => $path } );
      $drop{$id_str}= $removed if (@$removed);
    }
    return \%drop;
  }
  elsif ($be eq 'MongoDB')
  {
    die ("implement MongoDB remove");
  }
}

=end comment
=cut

sub check_policy
{
  my $objreg= shift;

  # print __LINE__, " objreg: ", main::Dumper ($objreg);

  my ($be, $policy)= map { $objreg->{'cfg'}->{$_} } qw(backend policy);
  unless ($be eq 'MongoDB')
  {
    print "not implemented for backend [$be]\n";
    return undef;
  }

  unless (defined ($policy))
  {
    print "no policy defined\n";
    return undef;
  }

  print __LINE__, " policy: ", main::Dumper ($policy);
  my ($key, $rs_list, $check_list, $ign_keys, $ign_paths)= map { $policy->{$_} } qw(key replica_sets check ignore_key ignore_path_pattern);
  my @check_list= @$check_list;
  my %ign_keys; %ign_keys= map { $_ => 1 } @$ign_keys if (defined ($ign_keys));
  my $replica_map= get_replica_map ($rs_list);

# my $MIN_SIZE= 2_000_000_000;
my $MIN_SIZE=   200_000_000;
  my $cursor= $objreg->{'_cat'}->find ( { fs_size => { '$gt' => $MIN_SIZE } } );

  my %items= ();
  my $item_count= 0;
  ITEM: while (my $item= $cursor->next())
  {
    my $kv= $item->{$key};
    next ITEM if (exists ($ign_keys{$kv}));

    my $path= $item->{path};
    foreach my $ign_path (@$ign_paths)
    {
      next ITEM if ($path =~ m#$ign_path#);
    }

    # print __LINE__, " key=[$key] kv=[$kv] item: ", main::Dumper ($item);

    my $rec= $items{$kv};
    if (defined ($rec))
    {
      # print "duplicate key...\n";
      my $mismatch= 0;
      foreach my $an (@check_list) { $mismatch++ if ($rec->{$an} ne $item->{$an}); }

      if ($mismatch)
      {
        print "item mismatch on key=[$key]: rec: ", main::Dumper ($rec), "item: ", main::Dumper ($item);
        next ITEM;
      }
    }
    else
    {
      my %rec= map { $_ => $item->{$_} } (@check_list, qw(fileinfo));
      $rec= $items{$kv}= \%rec;
    }

    push (@{$rec->{stores}->{$item->{store}}}, $path);
    $item_count++;
  }
  print __LINE__, " item_count: $item_count\n";
  # print __LINE__, " items: ", main::Dumper (\%items);

  foreach my $kv (keys %items)
  {
    my $item= $items{$kv};
    check_replication_policy ($replica_map, $item);
  }
}

sub get_replica_map
{
  my $rs_list= shift;
  print __LINE__, " rs_list: ", main::Dumper ($rs_list);

  my $map=
  {
    stores => {},
    store_count => {},
  };

  foreach my $rs (keys %$rs_list)
  {
    foreach my $store (@{$rs_list->{$rs}})
    {
      $map->{stores}->{$store}= $rs;
      $map->{store_count}->{$rs}++;
    }
  }

  print __LINE__, " map: ", main::Dumper ($map);
  $map;
}  

sub check_replication_policy
{
  my $map= shift;
  my $item= shift;

  # print __LINE__, ' item: ', main::Dumper ($item);
  # print __LINE__, ' map: ', main::Dumper ($map);
  my @diag;

  my %replica_sets;
  my $stores= $item->{stores};
  foreach my $store (keys %$stores)
  {
    # print __LINE__, " store=[$store]\n";
    my @paths= $stores->{$store};
    if (@paths > 1)
    {
      push (@diag, [ 'store_duplicate', $store ]);
    }

    if (exists ($map->{stores}->{$store}))
    {
      my $set= $map->{stores}->{$store};
      $replica_sets{$set}++;
    }
    else
    {
      push (@diag, [ 'store_not_in_replica_set', $store ]);
    }
  }

  my $prefered_replica_set;
  my @replica_sets= sort keys %replica_sets;
     if (@replica_sets == 0) { push (@diag, [ 'no_replica_set' ]); }
  elsif (@replica_sets >  1) { push (@diag, [ 'multiple_replica_sets', join (' ', @replica_sets) ]); }
  elsif (@replica_sets == 1)
  { # NOTE: the object is in one replica set, now check, if it is present in all stores;

    # TODO: it should be possible to have say 10 stores in one replica set
    # and specify that an object must be present in at least 3 of them.
    # Right now, the map does not contain this info.  Maybe the config
    # should specify the real # map and not simple lists of replica sets ...

    # TODO: the replication map should also encode geographically
    # distributed replicas.  E.g. have at least 3 replicas on different
    # locations

    $prefered_replica_set= $replica_sets[0];

    if ($replica_sets{$prefered_replica_set} != $map->{store_count}->{$prefered_replica_set})
    {
      my @missing;
      my $ms= $map->{stores};
      foreach my $store (sort keys %$ms)
      {
        push (@missing, $store) if ($ms->{$store} eq $prefered_replica_set && !exists ($stores->{$store}));
      }

      push (@diag, [ 'replica_set_incomplete', join (' ', @missing) ]);
    }
  }

  # NOTE: there are different kings of problems:
  # * an object with too few replicas in a given replica set is a problem
  # * an object with all replicas but with extra copies is not really a problem, but should be noted

  if (@diag)
  {
    print "ATTN: replication policy problem; prefered_replica_set=[$prefered_replica_set]\n";
    print join ("\n", map { 'NOTE: ' . join (' ', @$_) } @diag), "\n";
    print __LINE__, ' item: ', main::Dumper ($item);
  }

  {
    prefered_replica_set => $prefered_replica_set,
    diag => \@diag,
  }
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

  my $be= $reg->{'cfg'}->{'backend'};

  if ($be eq 'TA::Hasher')
  {
    my $f= $reg->{'seq_file'};
    open (F_SEQ, '>:utf8', $f) or die "cant write sequence to '$f'";
    print F_SEQ encode_json ($reg->{'seq'}), "\n";
    close (F_SEQ);
  }
  else
  {
    $reg->{'_maint'}->update ( { 'an' => 'seq' }, { 'an' => 'seq', 'av' => $reg->{'seq'} }, { 'upsert' => 1 } );
  }
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

=begin comment

  my %cmm_c= map { $_ => $cmm->{$_} } qw(host username password db_name);
  # print "cmm_c: ", main::Dumper (\%cmm_c);

  my ($db, $col0, $col1, $col2);
  eval
  {
    my $connection= new MongoDB::MongoClient( %cmm_c );
    print "connection=[$connection]\n";
    # $connection->authenticate($cmm->{'db'}, $cmm->{'user'}, $cmm->{'pass'});
    $db= $connection->get_database($cmm->{'db_name'});

    $col0= $db->get_collection($cmm->{'maint'});
    $col1= $db->get_collection($cmm->{'catalog'});
    $col2= $db->get_collection($cmm->{'keys'});
    # print "col: [$col0] [$col1] [$col2]\n";
  };

  if ($@)
  {
    print "ATTN: can't connect to MongoDB ", (join ('/', map { $cmm->{$_} } qw(host user maint))), "\n";
    return undef;
  }

=end comment
=cut

  my $db= Util::MongoDB::connect ($cmm);
  return undef unless (defined ($db));
  # print "db: ", main::Dumper($db); exit;

  # TODO: streamline ...
  my $col0= $db->get_collection($cmm->{'maint'});
  my $col1= $db->get_collection($cmm->{'catalog'});
  my $col2= $db->get_collection($cmm->{'keys'});
  # print "col: [$col0] [$col1] [$col2]\n";

  $obj->{'_mongo'}= $db;
  $obj->{'_maint'}= $col0;
  $obj->{'_cat'}= $col1;
  $obj->{'_keys'}= $col2;

  1;
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

=head2 ($data, $removed)= $objreg->ta_remove ($key, $filter)

remove that items that match the filter; returns new record and a array-ref of removed items

=cut

sub ta_remove
{
  my $reg= shift;
  my $id_str= shift;
  my $filter= shift;

  my ($r, $fnm)= $reg->ta_retrieve ($id_str);
  # print "id_str=[$id_str] fnm=[$fnm] r: ", main::Dumper ($r);

  return undef unless (defined ($r)); # this item has possibly been deleted already

  my ($m, $n)= ta_filter ($r, $filter);

  if (@$m && @$n) # something filtered, something removed, so we need to update that file
  {
    $r->{'entries'}= $m;

    my $j= encode_json ($r);
    # print "generated json: [$j]\n";
    open (J, '>:utf8', $fnm); print J $j; close (J);
  }
  elsif (!@$m && @$n) #
  {
    print "nothing left to be saved; deleting file [$fnm]\n";
    unlink ($fnm);
    $r= undef;
  }
  else
  {
    print "nothing removed; no change\n";
  }

  ($r, $n);
}

=head1 INTERNAL FUNCTIONS

=head2 ($entry, $index)= ta_match ($data, $search)

Select first $entry from $data that matches hash ref $search.

=cut

sub ta_match
{
  my $all_reg= shift;
  my $search= shift;

  my @k= keys %$search;
  my @e= @{$all_reg->{'entries'}};
  REG: for (my $i= 0; $i <= $#e; $i++)
  {
    my $reg= $e[$i];
    foreach my $k (@k)
    {
      next REG unless ($reg->{$k} eq $search->{$k});
    }
    # print "found match: ", main::Dumper ($reg);
    return ($reg, $i);
  }
  return (undef, 0);
}

=head2 (\@matching, \@not_matching)= ta_filter ($data, $search)

Returns two sets of rows, those that match the search reocord and those that do not.

=cut

sub ta_filter
{
  my $all_reg= shift;
  my $search= shift;

  my @k= keys %$search;
  my @e= @{$all_reg->{'entries'}};
  my @m= ();
  my @n= ();
  REG: for (my $i= 0; $i <= $#e; $i++)
  {
    my $reg= $e[$i];
    foreach my $k (@k)
    {
      unless ($reg->{$k} eq $search->{$k})
      {
        push (@n, $reg);
        next REG;
      }
    }
    # print "found match: ", main::Dumper ($reg);
    push (@m, $reg);
  }

  return (\@m, \@n);
}

1;
__END__

=head1 ENVIRONMENT

=head1 TODOs

* this is a stub for storage in a local filesystem
** connect to centralized connection service


