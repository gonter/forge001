#!/usr/bin/perl
# $Id: $
# $URL: $

use strict;

package TA::Hasher;

use Data::Dumper;
$Data::Dumper::Indent= 1;

my %known_algorithms= map { $_ => 1 } qw(NULL S3C2L P3C3L);

sub new
{
  my $class= shift;
  my %par= @_;

  my $obj= {};
  foreach my $par (keys %par)
  {
    $obj->{$par}= $par{$par};
  }
  bless $obj, $class;

  my $algorithm= $obj->{'algorithm'};
  unless (defined ($algorithm) && exists ($known_algorithms{$algorithm}))
  {
    $algorithm= $obj->{'algorithm'}= 'NULL';
  }

  if ($algorithm eq 'S3C2L')
  {
    print "setting up S3C2L\n";
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

=head1 METHODS

=head2 $hasher->check_file ($name, $create);

find out if this file is in the archive, if $create is true, prepare path unless exists

 * create path object
 * check if the path elements exist
 * returns: ( $status, $mkpo );

=cut

sub check_file
{
  my $obj= shift;
  my $fnm= shift;
  my $create= shift;

  my $mkpo= &{$obj->{'mkpo'}}($fnm);
  # print "mkpo: fnm=[$fnm] ", Dumper ($mkpo);

  my @dir_path= @{$mkpo->{'L'}};
  unshift (@dir_path, $obj->{'pfx'}) if (exists ($obj->{'pfx'}));
  push (@dir_path, $fnm) if ($obj->{'name'} == 'dir');

  my $dir_path= join ('/', @dir_path);
  my $existed= (-d $dir_path) ? 1 : 0;

  if ($create)
  {
    &r_mkdir (@dir_path);
  }

  ($existed, $dir_path, \@dir_path);
}

sub r_mkdir
{
  my @dir_path= @_;

  my $fp= shift (@dir_path);
  my $c= 0;
  my $p;
  while (1)
  {
    unless (-d $fp)
    {
      print "mkdir [$fp]\n";
      mkdir ($fp);
      $c++;
    }

    my $p= shift (@dir_path);
    last unless (defined ($p));
    $fp .= '/'. $p;
  }

  $c;
}

package TA::Hasher::NULL;

sub mkpo
{
  my $S= shift;
  return undef unless (defined ($S));
  return { 'L' => [] };
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

__END__

=head1 TODO

=over 2

=item check_path ($mkpo)

=back

