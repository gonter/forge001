
package TA::Util;

use strict;

use JSON;

=head2 slurp_file ($filename, $format)

read contents of that file and 

=cut

sub slurp_file
{
  my $fnm= shift;
  my $format= shift || 'lines';

  open (FI, '<:utf8', $fnm) or return undef;
  my @lines= <FI>;
  close (FI);

  if ($format eq 'array')
  {
    return @lines;
  }
  elsif ($format eq 'arrayref')
  {
    return \@lines;
  }
  elsif ($format eq 'string')
  {
    return join ('', @lines);
  }
  elsif ($format eq 'json')
  {
    my $str= join ('', @lines);
    return decode_json ($str);
  }
  elsif ($format eq 'csv')
  {
    my $hdr= split (';', shift (@lines));
    my @d;
    while (my $l= shift (@lines))
    {
      push (@d, split (';', $l));
    }
    return [$hdr, \@d];
  }
 
  print STDERR "unknown slurp format '$format'\n";
  return undef;
}

1;

