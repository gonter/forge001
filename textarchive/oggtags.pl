#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use FileHandle;

binmode( STDOUT, ':utf8' ); autoflush STDOUT 1;
binmode( STDERR, ':utf8' ); autoflush STDERR 1;
binmode( STDIN,  ':utf8' );

my $dir= shift(@ARGV) || '.';

my $res= scan_ogg_dir($dir);
print __LINE__, " res: ", Dumper($res);

print_info ($res);
exit;

sub print_info
{
  my $d= shift;

  foreach my $track (@$d)
  {
    printf ("%2d %s\n", $track->{TRACKNUMBER}, $track->{TITLE});
  }
}

sub scan_ogg_dir
{
  my $dir= shift || '.';

  chdir($dir) unless ($dir eq '.');
  my @cmd= ('ogginfo', '*.ogg');

  open(OI, '-|:utf8', "ogginfo *.ogg") or die;
  my @res=();
  my $track;
  my $st= undef;
  while (<OI>)
  {
    chop;
    print __LINE__, " st=[$st] l=[$_]\n";
    if (m#^Processing file "(.*)"...#)
    {
      my $fnm= $1;
      $track= { filename => $fnm };
      push (@res, $track);
      $st= undef;
    }
    elsif ($_ eq 'User comments section follows...')
    {
      $st= 'UCS';
    }
    elsif ($_ =~ m#Vorbis stream (\d+):#)
    {
      my $stream_num= $1;
      $st= 'SI';
    }
    elsif ($st eq 'UCS' && m#^[\t ]+([A-Z_]+)=(.+)#)
    {
      my ($label, $value)= ($1, $2);
      print __LINE__, " label=[$label] value=[$value]\n";
      $track->{$label}= $value;
    }
  }
  close(OI);

  \@res;
}

