#!/usr/bin/perl

use strict;

use MP3::Info;
use Data::Dumper;
$Data::Dumper::Indent= 1;

my @PAR;
while (my $arg= shift (@ARGV))
{
  if ($arg =~ /^-/)
  {
    &usage;
  }
  else
  {
    push (@PAR, $arg);
  }
}

foreach my $file (@PAR)
{
  my $tag= get_mp3tag ($file);

  print "file: [$file]\n";
  print "tag: ", Dumper ($tag);
}


