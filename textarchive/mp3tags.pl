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


__END__

=head1 Dependencies

Ubuntu:
  apt-get install libmp3-info-perl

=begin comment

there is more interesting stuff in mp3 files, for example:

nbgg:gonter> mp3tags.pl '_in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3'
file: [_in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3]
tag: $VAR1 = {
  'YEAR' => '1987',
  'ARTIST' => 'Roger Waters',
  'COMMENT' => '',
  'ALBUM' => 'Radio K.A.O.S.',
  'TITLE' => 'The Tide Is Turning',
  'GENRE' => 'Progressive Rock',
  'TRACKNUM' => '8',
  'TAGVERSION' => 'ID3v2.3.0'
};
nbgg:gonter> mp3info '_in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3'
_in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3 does not have an ID3 1.x tag.

nbgg:gonter> mp3info -x '_in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3'
_in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3 does not have an ID3 1.x tag.
File: _in_2013-05-25_Rock/Roger Waters/Radio K.A.O.S/08-The Tide Is Turning.mp3
Media Type:  MPEG 1.0 Layer III
Audio:       320 kbps, 44 kHz (joint stereo)
Emphasis:    none
CRC:         No
Copyright:   No
Original:    No
Padding:     Yes
Length:      5:44

=end comment
=cut

