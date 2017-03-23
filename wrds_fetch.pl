#!/usr/bin/env perl
use DBI;
use Getopt::Long;
use Time::localtime;
use Env qw($PGDATABASE $WRDS_ID);

################################################
# 0. Get command-line arguments                #
################################################

# Extract options from the command line
# Example ./wrds_fetch.pl comp.idx_index --fix-missing --wrds-id iangow
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
my $fix_missing = '';
my $updated = '';
my $drop = '';
my $rename = '';
GetOptions('fix-cr' => \$fix_cr,
           'fix-missing' => \$fix_missing,
           'wrds-id=s' => \$wrds_id,
           'dbname=s' => \$dbname,
           'updated=s' => \$updated,
           'fix-cr' => \$fix_cr,
           'drop=s' => \$drop,
           'obs=s' => \$obs,
           'rename=s' => \$rename);


# Get schema and table name from command line. I have set my database
# up so that these line up with the names of the WRDS library and data
# file, respectively.
@table_args = split(/\./,  @ARGV[0]);

$db_schema = $table_args[0];
$table_name = $table_args[1];
$pg_table = $table_name;

if ($rename ne '') {
    $rename_str = "rename=($rename)"
} else {
    $rename_str = "";
}



if ($obs ne '') {
    $obs_str = "obs=$obs"
} else {
    $obs_str = "";
}

if ($drop ne '') {
    $drop_str = "drop=$drop"
} else {
    $drop_str = "";
}

if ($obs ne '' || $drop ne '' || $rename_str) {
    $table_name = "$table_name($drop_str $obs_str $rename_str)";
}

$db = "$db_schema.";

# Use the quarterly update of CRSP
$db =~ s/^crsp/crspq/;

################################################
# 1. Get format of variables on WRDS table     #
################################################

# SAS code to extract information about the datatypes of the SAS data.
# Note that there are some date formates that don't work with this code.
$sas_code = "
    options nonotes nosource;

    libname pwd '.';

    * Edit the following to refer to the table of interest;
    %let db=$db;
    %let table_name=$pg_table;

    data pwd.schema;
        set $db$pg_table(drop=$drop obs=1 $rename_str);
    run;

    * Use PROC CONTENTS to extract the information desired.;
    proc contents data=pwd.schema out=schema noprint;
    run;

    * Do some preprocessing of the results;
    data schema(keep=name postgres_type);
      set schema(keep=name format formatl formatd length type);
      format postgres_type \\\$36.;
      if prxmatch('/datetime/i', format) then postgres_type='timestamp';
      else if format='TIME8.' or prxmatch('/time/i', format)
          then postgres_type='time';
      else if format='YYMMDDN' or format='DATE9.'
              or prxmatch('/date/i', format)
              or prxmatch('/yymmdd/i', format)
              then postgres_type='date';
          else if format='BEST' then postgres_type='float8';
          else if type=1 then do;
        if formatd ^= 0 then postgres_type = 'float8';
        if formatd = 0 and formatl ^= 0 then postgres_type = 'int8';
        if formatd = 0 and formatl =0 then postgres_type = 'float8';
        end;
        else if type=2 then postgres_type = 'text';
    run;

    * Now dump it out to a CSV file;
    proc export data=schema outfile=stdout dbms=csv;
    run;";

# Run the SAS code on the WRDS server and save the result to @result
$cmd = "echo \"$sas_code\" | ";
$cmd .= "ssh -C $wrds_id\@wrds-cloud.wharton.upenn.edu 'qsas -stdio -noterminal ' 2>/dev/null";
@result = `$cmd`;

# Now fill an array with the names and data type of each variable
my %var_type;
foreach $row (@result)    {
    my @fields = split(",", $row);
    my $field = @fields[0];

    # Rename fields with problematic names
    $field =~ s/^do$/do_/i;

    my $type = @fields[1];
    chomp $type;
    if ($type eq "timestamp") {
        # Need to fix timestamps below
        $var_type{$field} = "text";
    } else {
       $var_type{$field} = $type;
    }
    $true_var_type{$field} = $type;
}

##################################################
# 2. Get column order of variables on WRDS table #
##################################################


# Get the first row of the SAS data file from WRDS. This is important,
# as we need to put the table together so as to align the fields with the data
# (the "schema" code above doesn't do this for us).

$sas_code = "
    options nosource nonotes;

    proc export data=pwd.schema
        outfile=stdout
        dbms=csv;
    run;";

# Run the SAS command and get the first row of the table
$cmd = "echo \"$sas_code\" | ";
$cmd .= "ssh -C $wrds_id\@wrds-cloud.wharton.upenn.edu 'qsas -stdio -noterminal ' 2>/dev/null";
$cmd .= "| head -n 1";
print "Getting schema for $db_schema.$table_name\n";
$row = `$cmd`;

##################################################
# 3. Construct and run CREATE TABLE statement    #
##################################################

$sql = "CREATE TABLE $db_schema.$pg_table (";

# Set some default/initial parameters
$first_field = 1;
$sep="";

# Construct SQL fragment associated with each variable for
# the table creation statement
foreach $field (split(',', $row)) {

    chomp $field;
    # Rename fields with problematic names
    $field =~ s/^do$/do_/i;

    # Concatenate the component strings. Note that, apart from the first
    # field a leading comma is inserted to separate fields in the
    # CREATE TABLE SQL statement.
    $sql .= $sep . $field . " " . $var_type{$field};
    if ($first_field) { $sep=", "; $first_field=0; }
}
$sql .=  ");";

# Connect to the database
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname")
    or die "Cannot connect: " . $DBI::errstr;

$dbh->do("SET search_path TO $db_schema");

# Drop the table if it exists already, then create the new table
# using field names taken from the first row
$dbh->do("
    CREATE SCHEMA IF NOT EXISTS $db_schema;
    DROP TABLE IF EXISTS $pg_table CASCADE;");
$dbh->do($sql);

##################################################################
# 4. Import the data using COPY from CSV file piped from WRDS    #
##################################################################

$tm = localtime;
printf "Beginning file import at %d:%02d:%02d\n",@$tm[2],@$tm[1],@$tm[0];

if ($fix_cr) {
    $fix_missing=1;
    $fix_cr_code = "
        array _char _character_;
        do over _char;
            _char = compress(_char, , 'kw');
        end;"
} else {
    $fix_cr_code = "";
}

if ($fix_missing | $drop ne '' | $obs ne '') {
    # If need to fix special missing values, then convert them to
    # regular missing values, then run PROC EXPORT
    $dsf_fix =  ($table_name eq "dsf" ? "format numtrd 8.;\n" : "");
    $fund_names_fix =  ($table_name eq "fund_names" ? "proc sql;\n DELETE FROM pwd.$wrds_id$pg_table\n WHERE prxmatch('\\D', first_offer_dt) ge 1;\n quit;\n" : "");

    $sas_code = "
      options nosource nonotes;

      libname pwd '/sastemp';

      * Fix missing values;
      data pwd.$wrds_id$pg_table;
          set $db$pg_table($drop_str $obs_str $rename_str);
          $dsf_fix
	      $fix_cr_code

          array allvars _numeric_ ;

          do over allvars;
              if missing(allvars) then allvars = . ;
          end;

      run;

      $fund_names_fix

      proc export data=pwd.$wrds_id$pg_table outfile=stdout dbms=csv;
      run;";

} else {
  # Otherwise, just use PROC EXPORT
  $sas_code = "
      options nosource nonotes;

      proc export data=$db$table_name outfile=stdout dbms=csv;
      run;";

}

# Use PostgreSQL's COPY function to get data into the database
$cmd = "echo \"$sas_code\" | ";
$cmd .= "ssh -C $wrds_id\@wrds-cloud.wharton.upenn.edu 'qsas -stdio -noterminal' 2>/dev/null | ";
$cmd .= "psql -d $dbname -c \"COPY $db_schema.$pg_table FROM STDIN CSV HEADER ENCODING 'latin1' \"";

print "Importing data into $db_schema.$table_name.\n";
$result = system($cmd);
print "Result of system command: $result\n";

$tm=localtime;
printf "Completed file import at %d:%02d:%02d\n",@$tm[2],@$tm[1],@$tm[0];

# Fix date times. Imported as text and converted to PostgreSQL
# timestamps using regular expressions.
foreach $field (split(',', $row)) {

    chomp $field;
    #
    if ($true_var_type{$field} eq "timestamp") {
      $sql = "ALTER TABLE $pg_table ALTER $field TYPE timestamp USING";
      $sql .= " regexp_replace($field,  '(\\d{2}[A-Z]{3}\\d{4}):', '\\1 ' )::timestamp";
      $dbh->do($sql);
    }
}

# Comment on table to reflect date it was updated
my ($day,$month,$year)=($tm->mday(),$tm->mon(),$tm->year());
if ($updated eq "") {
    $updated = sprintf( "Table updated on %d-%02d-%02d.", 1900+$year, 1+$month, $day);
}
print "$updated\n";
$dbh->do("COMMENT ON TABLE $pg_table IS '$updated'");
$dbh->disconnect();

