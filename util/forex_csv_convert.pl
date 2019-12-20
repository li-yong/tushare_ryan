# Convert csv format, from Taobao to pyalgotrading.
use strict;
use warnings;
use File::Basename;
use Getopt::Std;
use File::Spec;

my %options=();
getopts("f:s:", \%options);

print "Input file -f $options{f}\n";

#my $file="/oandapybot-ubuntu/backtest/data/XAUUSD/XAUUSD_2016_100f.csv";
my $file=$options{f};

my $basename=basename($file);
my $abspath=File::Spec->rel2abs($file);
my $basedir=dirname($abspath);
$basename =~m/(.*)\.txt/;

my $file_out = $basedir."/tmp/".$1.".txt";  #have to be txt for python script processing later.
print "file out is $file_out\n";

open (my $fh, '<', $file) or die "Could not open file $file, $!\n\r";
chomp(my @lines=<$fh>);
close $fh;


#my $newfileStr="Date Time,Open,High,Low,Close,Volume,Adj Close\n";
my $newfileStr="DateTime,Open,High,Low,Close,Volume\n";


#get from tb: 
#XAUUSD,20161018,183500,1261.8,1262.1,1261.8,1262.1,4

#pyalgo required:
#Date,Open,High,Low,Close,Volume,Adj Close
#2011-12-30,152.14,153.75,151.79,151.99,10852700,151.99



foreach my $l (@lines){
   $l =~s/\n//g;
   $l =~s/\r//g;
   
   if ($l =~m/DTYYYYMMDD/){
      next;
   }

   if($l =~m/(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)/){
      my $symbol=$1;
      my $date=$2;
      my $time=$3;
      my $open=$4;
      my $high=$5;
      my $low=$6;
      my $close=$7;
      my $vol=$8;

      $date=~m/(\d\d\d\d)(\d\d)(\d\d)/;
      my $yyyy=$1;
      my $mm=$2; my $dd=$3; 


      $time=~m/(\d\d)(\d\d)(\d\d)/;
      my $HH=$1; my $MM=$2; my $SS=$3;


      my  $t = "$symbol,${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS},${open},${high},${low},${close},${vol}\n";
      $newfileStr = $newfileStr.$t;

    #  print $t;
    #  print "====\n";
    #  print $newfileStr;
	  
   }


}


open(my $fho, '>', $file_out) or die "Could not write file $file, $!\n";
print $fho $newfileStr;
close $fho;

print "done, out file $file_out\n";



