#!/usr/bin/env perl
use DBI;
use Getopt::Long;
use Time::localtime;
use Env qw($PGDATABASE $PGUSER $PGHOST $EDGAR_DIR $WRDS_ID);

################################################
# 0. Get command-line arguments                #
################################################

# Extract options from the command line
# Example ./get_wrds_data   .pl comp idx_index --fix-missing --wrds-id iangow
# gets comp.idx_index from WRDS using WRDS ID iangow. It also converts
# special missing values (e.g., .Z) to regular missing values (i.e., .)
#
# In most cases, you will want to omit --fix-missing.
#
# Database name can be specified on command line using
# --dbname=your_database, otherwise environment variable
# PGDATABASE will be used.
# optional variable with default value
my $wrds_id = $WRDS_ID;
my $dbname = $PGDATABASE;
my $force = '';
my $fix_missing = '';
my $fix_cr = '';

GetOptions('force' => \$force,
            'fix-missing' => \$fix_missing,
            'wrds-id=s' => \$wrds_id,
            'dbname=s' => \$dbname,
            'fix-cr' => \$fix_cr,
            'drop=s' => \$drop,
            'obs=s' => \$obs);

# Get schema and table name from command line. I have set my database
# up so that these line up with the names of the WRDS library and data
# file, respectively.
@table_args = split(/\./,  @ARGV[0]);

$db_schema = $table_args[0];
$table_name = $table_args[1];

################################################
# 1. Get comments from PostgreSQL database     #
################################################
## Connect to the database
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname")
    or die "Cannot connect: " . $DBI::errstr;

# Prepare a query to get the comment from a table
# comment = description, it seems
my $sth = $dbh->prepare('
    SELECT description
    FROM pg_description
    JOIN pg_class
    ON pg_description.objoid = pg_class.oid
    JOIN pg_namespace
    ON pg_class.relnamespace = pg_namespace.oid
    WHERE relname = ? AND nspname=?')
    or die "Couldn't prepare statement: " . $dbh->errstr;

# Execute the query
$sth->execute($table_name, $db_schema)
        or die "Couldn't execute statement: " . $sth->errstr;

# Read the matching records and print them out
@data = $sth->fetchrow_array();
$comment = $data[0];

$sth->finish;

$dbh->disconnect;

################################################
# 2. Get modified date from WRDS               #
################################################
# Use the quarterly update of CRSP
$db = $db_schema;
$db =~ s/^crsp/crspq/;

$sas_code = "proc contents data=".  $db . "." . $table_name .";";
$cmd = "echo \"$sas_code\" | ";
$cmd .= "ssh -C $wrds_id\@wrds.wharton.upenn.edu ";
$cmd .= "'sas -stdio -noterminal' 2>/dev/null";

@result = `$cmd`;

my $modified = "";
my $next_row = 0;
foreach (@result) {
    if ($next_row==1) {
        # print "$_";
        $_ =~ s/^\s+(.*)\s+$/$1/;
        chomp $_;
        $_ =~ s/\s+$//;
        if ($_ !~ "Protection") {
          $modified .= " " . $_;
        }
        $next_row = 0;
    }

    if ($_ =~ /Last Modified/) {
        $_ =~ s/^Last Modified\s+(.*?)\s{2,}.*$/Last modified: $1/;
        chomp $_;
        $modified .= $_;
        $next_row = 1;
    }
}

$modfied =~ s/\s+$//;

################################################
# 3. If updated table available, get from WRDS #
################################################
if ($modified ne $comment || $force) {
    $cmd = "./wrds_fetch.pl $db_schema.$table_name";
    $cmd .= ($fix_missing eq '' ? '' : ' --fix-missing');
    $cmd .= ($fix_cr eq '' ? '' : ' --fix-cr');
    $cmd .= ($obs eq '' ? '' : " --obs=$obs");
    $cmd .= ($drop eq '' ? '' : " --drop='$drop'");
    $cmd .= " --wrds-id=$wrds_id --dbname=$dbname";
    $cmd .= " --updated=\"$modified\"";
    if ($force) {
        print "Forcing update based on user request.\n";
    } else {
        print "Updated $db_schema.$table_name is available.";
        print " Getting from WRDS.\n";
    }
    system($cmd);
    exit 1;
} else {
    print "$db_schema.$table_name already up to date\n";
    exit 0;
}
