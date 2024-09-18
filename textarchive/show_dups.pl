#!/usr/bin/perl

use strict;
use Data::Dumper;
$Data::Dumper::Indent= 1;
$Data::Dumper::SortKeys= 1;

my $super_cat= SuperCat->new();
print __LINE__, " super_cat: ", Dumper($super_cat);

my @PARS= ();
while (my $arg= shift (@ARGV))
{
  if ($arg =~ /^-/)
  {
    die "no options implemented";
  }
  else
  {
    push (@PARS, $arg);
  }
}

foreach my $par (@PARS)
{
  $super_cat->read_catalog($par);
}

# print __LINE__, " super_cat: ", Dumper($super_cat);
print __LINE__. " cats: ", Dumper($super_cat->{cats});
$super_cat->show_dups();

exit(0);

package SuperCat;

sub new { bless { keys => {}, cats => {}, count => 0 }, shift }

sub read_catalog
{
  my $self= shift;
  my $cat= shift;

  if (exists ($self->{cats}->{cat}))
  {
    return undef;
  }

  my $cat_num= ++$self->{count};
  my $cat_info=
  {
    cat_number => $cat_num,
    cat_name   => $cat,
  };
  $self->{cats}->{$cat}= $cat_info;

  # push (@{$self->{cats}}

  die "can't read cat=[$cat]" unless (open (CAT, '<:utf8', $cat));

  my $keys= $self->{keys};
  while (<CAT>)
  {
    chop;
    my ($md5, $x_file, $size, $path)= split(' ', $_, 4);
    # print __LINE__, " md5=[$md5], size=[$size], path=[$path]\n";

    my $x;
    if (exists ($keys->{$md5})) { $x= $keys->{$md5} }
    else
    {
      $x= $keys->{$md5}=
      {
        key => $md5,
        count => 0,
        cats => {},
      };
    }

    $x->{count}++;
    push (@{$x->{cats}->{$cat_num}}, $path);
  }
  close(CAT);
}

sub show_dups
{
  my $self= shift;

  foreach my $key (keys %{$self->{keys}})
  {
    my $x= $self->{keys}->{$key};
    # print main::Dumper($x);
    if ($x->{count} != 1)
    {
      print main::Dumper($x);
    }
  }
}

