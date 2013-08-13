#!/usr/bin/perl

use lib 'lib';

use TA::Hasher;
use Data::Dumper;
$Data::Dumper::Indent= 1;

my @t1= qw( A075-01001-AC10800264 A075-01002-AC10784764
  A075-01003-AC10804771 A075-01004-AC03756249 A075-01005-AC10790148
  A075-01006-AC04357238 A075-01007-AC10799762
);

my $ta= new TA::Hasher ('algorithm' => 'S3C2L', 'pfx' => 'tmp', 'name' => 'dir');
print "ta: ", Dumper ($ta);

foreach my $x (@t1)
{
  if ($x =~ m#(AC\d{8})$#)
  {
    my $AC= $1;
    my @r= $ta->check_file ($AC, 1);
    print "x=[$x] AC=[$AC] r=", Dumper (\@r);
  }
}

