#!/usr/bin/perl

use strict;

my $do_zip= 1;
my $do_bup= 1;

sub new
{
  my $class= shift;

  my $self= {};
  bless $self, $class;
  $self->set(@_);
  $self;
}

sub set
{
  my $self= shift;
  my %par= @_;
  foreach my $par (keys %par)
  {
    $self->{$par}= $par{$par};
  }
  $self;
}

sub mk_mysql_dump
{
  my $self= shift;
  my $fnm= shift;
  my @rest= @_;

  my ($user, $pass, $db_name)= map { $self->{$_} or die } qw(user pass db);

  $fnm= sprintf ('%s_%s.dump', $db_name, ts_ISO()) unless ($fnm);

  print "saving to fnm=[$fnm]\n";
  my @cmd1= ('/usr/bin/mysqldump', '-u', $user);
  push (@cmd1, "--password=$pass");
  # push (@cmd1, '-p', $pass); # does not work that way!
  my $idx_p= $#cmd1;
  push (@cmd1, @rest);
  push (@cmd1, '-r', $fnm, $db_name);

  my @cmd2= @cmd1;
  $cmd2[$idx_p]= "--password='xxxx'";
  # $cmd2[$idx_p]= 'xxxx';

  print "cmd: [", join (' ', @cmd2), "]\n";

  if ($do_bup)
  {
    system (@cmd1);
    print "\n\n";
    system ('gzip', '-v', $fnm) if ($do_zip);
  }
}

sub ts_ISO
{
  my $t= shift || time ();

  my @t= localtime ($t);

  sprintf ("%4d-%02d-%02dT%02d%02d%02d", $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0]);
}

1;

