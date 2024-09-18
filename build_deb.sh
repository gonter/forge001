#!/bin/sh

epoch=0
pkgname=`awk '/Package:/ {print $2}' debian/control`
version=`awk '/Version:/ {print $2}' debian/control`
pkgdir=tmp/$pkgname/$epoch/$version
bindir=$pkgdir/data/usr/bin

pmlnk.pm --copy --target $pkgdir/data/usr/share/perl5 .
mkdir $pkgdir/control
cp debian/control $pkgdir/control
echo '2.0' > $pkgdir/debian-binary

mkdir -p $bindir
cp textarchive/vlib001.pl textarchive/mp3tags.pl textarchive/oggtags.pl textarchive/show_dups.pl $bindir

mkdeb.pl $pkgdir

