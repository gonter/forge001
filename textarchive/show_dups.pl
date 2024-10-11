#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;
$Data::Dumper::SortKeys= 1;

use Util::JSON;

my $super_cat= SuperCat->new();
# print __LINE__, " super_cat: ", Dumper($super_cat);

my @PARS= ();
my $report= 'report';
while (my $arg= shift (@ARGV))
{
     if ($arg eq '-')  { push (@PARS, '-'); }
  elsif ($arg eq '--') { push (@PARS, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($opt, $val)= split ('=', $1, 2); 
    if ($opt eq 'help') { usage(); }
    elsif ($opt eq 'report') { $report= $val || shift(@ARGV) }
    else { usage(); }
  }
  elsif ($arg =~ /^-(.+)/)
  {
    foreach my $opt (split ('', $1))
    {
         if ($opt eq 'h') { usage(); exit (0); }
    # elsif ($opt eq 'x') { $x_flag= 1; }
      else { usage(); }
    }
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
Util::JSON::write_json_file($report . '.json', $super_cat);

# print __LINE__. " cat_names: ", Dumper($super_cat->{cat_names});

$super_cat->show_dups($report . '.txt');

exit(0);

package SuperCat;

sub new { bless { keys => {}, cat_names => {}, cat_list => [], cat_number => 0 }, shift }

sub read_catalog
{
  my $self= shift;
  my $cat= shift;

  if (exists ($self->{cat_names}->{cat}))
  {
    return undef;
  }

  my $cat_number= ++$self->{cat_number};
  my $cat_info=
  {
    cat_number => $cat_number,
    cat_name   => $cat,
  };
  $self->{cat_names}->{$cat}= $cat_info;
  my $cc= $self->{cat_list}->[$cat_number]= { cat_info => $cat_info, keys => {} };
  my $ccl= $cc->{keys};

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
        instance_count => 0,
        cats => {},
        size => $size,
      };
    }

    $x->{instance_count}++;
    push (@{$x->{cats}->{$cat_number}}, $path);
    $ccl->{$md5}++;
  }
  close(CAT);
}

sub show_dups
{
  my $self= shift;
  my $report= shift;

  # print __LINE__, " show_dups\n";

  my $keys= $self->{keys};

  # go through all keys which belong to the first catalog and see, if that thing shows up in subsequent catalogs as well
  my $key_list_c1= $self->{cat_list}->[1]->{keys};
  my %source_only= ();
  my %target_also= ();
  foreach my $key (sort keys %$key_list_c1)
  {

    my $x= $keys->{$key};
    # print __LINE__, ' x: ', main::Dumper($x);
    if ($x->{instance_count} != 1)
    {
      my %cats= %{$x->{cats}};
      my @cats= keys %cats;
      if (@cats > 1)
      {
        # print __LINE__, ' (dup) x: ', main::Dumper($x);
        $target_also{$key}= $x;
      }
      else
      {
        $source_only{$key}= $x;
      }
    }
    else
    {
      $source_only{$key}= $x;
    }
  }

  my $show_source_only= 1;
  my $show_target_also= 1;

  my $cn= $self->{cat_names};
  my @cat_names;
  foreach my $n (keys %$cn)
  {
    my $x= $cn->{$n};
    $cat_names[$x->{cat_number}]= $x;
  }
  # print __LINE__, " cat_names (Array): ", main::Dumper(\@cat_names);

  open (REPORT, '>:utf8', $report) or die;

  print REPORT '='x72, "\n";
  print REPORT "source catalog: ", $cat_names[1]->{cat_name}, "\n";
  print REPORT '='x72, "\n";

  my $count_source_only= 0;
  if ($show_source_only)
  {
    print REPORT "only present in source catalog\n";
    print REPORT '-'x72, "\n";
    foreach my $key (sort keys %source_only)
    {
      my $x= $keys->{$key};
      # print __LINE__, " x: ", main::Dumper($x);
      show_x(*REPORT, $x);
      $count_source_only++;
    }

    print REPORT "\n\n";
  }

  my $count_target_also= 0;
  if ($show_target_also)
  {
    # *REPORT= *STDOUT; # for debugging, if needed

    print REPORT "also present in at least one of the target catalogs\n";
    print REPORT '-'x72, "\n";
    foreach my $key (sort keys %target_also)
    {
      my $x= $keys->{$key};
      # print __LINE__, " x: ", main::Dumper($x);
      show_x(*REPORT, $x);
      foreach my $c (sort { $a <=> $b } keys %{$x->{cats}})
      {
        next if ($c == 1);

        print REPORT "  * ", $cat_names[$c]->{cat_name}, ": ";
        my $y= $x->{cats}->{$c};
        foreach my $p (@$y) { print REPORT ' ', $p; }
        print REPORT "\n";
      }
      print REPORT "\n";

      $count_target_also++;
    }

    print REPORT "\n\n";
  }

  print REPORT '-'x72, "\n";
  printf REPORT ("Summary: %6d source_only, %6d target_also\n", $count_source_only, $count_target_also);
  printf        ("Summary: %6d source_only, %6d target_also\n", $count_source_only, $count_target_also);

  close (REPORT);
}

sub show_x
{
  local *REPORT= shift;
  my $x= shift;

  printf REPORT ("%s %9ld %4d", $x->{key}, $x->{size}, $x->{instance_count});

  my $y= $x->{cats}->{1};
  foreach my $p (@$y) { print REPORT ' ', $p; }
  print REPORT "\n";
}

