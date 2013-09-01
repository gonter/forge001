
package TA::Util;

use strict;

=head2 _file_slurp ($filename, $format)

read contents of that file and 

=cut

sub slurp_file
{
  my $fnm= shift;
  my $format= shift || 'lines';

  open (FI, $fnm) or return undef;
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
 
  print STDERR "unknown slurp format '$format'\n";
  return undef;
}

1;

