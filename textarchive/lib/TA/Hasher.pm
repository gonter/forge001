#!/usr/bin/perl
# $Id: $
# $URL: $

use strict;

package TA::Hasher;

sub new
{
  my $class= shift;
  my $label= shift;
  my $algorithm= shift;

  my $obj=
  {
    'label' => $label,
    'algorithm' => $algorithm,
  };
  bless $obj, $class;

  if ($algorithm eq 'S3C2L')
  {
    $obj->{'mkpo'}= \&TA::Hasher::S3C2L::mkpo;
  }
  elsif ($algorithm eq 'P3C3L')
  {
    $obj->{'mkpo'}= \&TA::Hasher::P3C3L::mkpo;
  }
  else
  {
    $obj->{'mkpo'}= \&TA::Hasher::NULL::mkpo;
  }

  $obj;
}

package TA::Hasher::NULL;

sub mkpo
{
  my $S= shift;
  return undef unless (defined ($S));
  return { 'L' => []; }
}

package TA::Hasher::S3C2L;

sub mkpo
{
  my $S= shift;

  return undef unless (defined ($S));

  my @L;

  if ($S =~ m#(.{1,3})(...)$#)
       { @L= ( $2, $1 ); }
  else { @L= ( 'ZZZ', $S ); }

  return { 'L' => \@L };
}

package TA::Hasher::P3C3L;

sub mkpo
{
  my $S= shift;

  return undef unless (defined ($S));

  my @L;

  if ($S =~ m#^(...)(...)(.{1,3})#)
       { @L= ( $1, $2, $3 ); }
  elsif ($S =~ m#^(...)(.{1,3})#)
       { @L= ( $1, $2, 'ZZZ' ); }
  else { @L= ( $S, 'ZZZ', 'ZZZ' ); }

  return { 'L' => \@L };
}

1;
