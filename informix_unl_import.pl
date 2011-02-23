#!/usr/bin/perl
use warnings;
use strict;

use Encode;
use DBI;
use Data::Dump qw(dump);

my $debug = $ENV{DEBUG} || 0;

my $dbfile = '/dev/shm/import.sqlite';

warn "# output in $dbfile";

unlink $dbfile if -e $dbfile;

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","");

foreach my $sql ( glob "knjiznica/*.sql" ) {
	warn "schema file: $sql\n";
	{
		local $/ = undef;
		open(my $fh, '<', $sql);
		$sql = <$fh>;
		$sql =~ s{DATETIME HOUR TO MINUTE}{TEXT}gs;
		while ( $sql =~ s{--[^\n\r]*[\n\r]}{\n}gs ) {}; # strip comments
	}
	foreach my $s ( split(/\)\s*;/, $sql) ) {
		$s .= ')';
		warn "$s\n";
		$dbh->do( $s ) || warn $dbh->errstr;
	}
}

foreach my $dump ( glob "knjiznica/*.unl" ) {
	my $table = $1 if $dump =~ m{/(\w+)\.unl};
	warn "# $dump\n";
	open(my $fh, '<', $dump);
	my $cont = '';
	my ( $delimiter, $cols );
	while( <$fh> ) {
		s{[\n\r]*$}{};
		my $line = decode('cp1250',$_);
		if ( $line =~ m/\\$/ ) {
			$cont .= $line;
			next;
		} elsif ( $cont ) {
			$line = $cont . $line;
			$cont = '';
		}
		$line =~ s/'/''/g;
		if ( ! $delimiter ) {
			$delimiter = $1 if $line =~ m/(#|\|)$/;
			$cols = $line =~ s{\Q$delimiter\E}{$delimiter}g;
		}
		die "can't find delimiter in $line" unless $delimiter;
		$line =~ s{\Q$delimiter\E$}{} || die "can't find end-line delimiter";
		my @v = split(/\Q$delimiter\E/,$line,$cols);
		warn "# $table $cols $delimiter [$line] ",dump(@v) if $debug;
		$dbh->do( "INSERT INTO $table VALUES ('" . join("','", @v) . "')" )
		|| die $dbh->errstr;
	}
}
