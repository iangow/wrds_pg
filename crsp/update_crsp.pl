#!/usr/bin/env perl
# chdir('..') or die "$!";
use Env qw($PGBACKUP_DIR);

# Update monthly data
$msf = system("./wrds_update.pl crsp.msf --fix-missing");
$msf = $msf >> 8;

$msi = system("./wrds_update.pl crsp.msi");
$msi = $msi >> 8;

$msedelist = system("./wrds_update.pl crsp.msedelist --fix-missing");

$mport = system("./wrds_update.pl crsp.mport1");

# See http://perldoc.perl.org/functions/system.html
$mport = $mport >> 8;
if ($mport) {
    print "Getting ermport1\n";
    system("crsp/get_ermport.pl");
    system("psql -f crsp/crsp_make_ermport1.sql");
}

$msedelist = $msedelist >> 8;

if ($msi) {
    system("psql -c 'CREATE INDEX ON crsp.msi (date)'");
}

if ($mport | $msf | $msi | $msedelist) {
    system("psql -f crsp/crsp_make_mrets.sql")
}

if ($msf) {
    system("psql -c 'CREATE INDEX ON crsp.msf (permno, date);'");
}

# Update daily data
$dsf = system("./wrds_update.pl crsp.dsf --fix-missing");
# See http://perldoc.perl.org/functions/system.html
$dsf = $dsf >> 8;

if ($dsf) {
    system("psql -c 'ALTER TABLE crsp.dsf ALTER permno TYPE integer;'");
    system("psql -c 'SET maintenance_work_mem=\"1999MB\"; CREATE INDEX ON crsp.dsf (permno, date)'");
}

$dsi = system("./wrds_update.pl crsp.dsi");
$dsi = $dsi >> 8;

if ($dsi) {
    # system("psql -f crsp/crsp_indexes.sql");
    system("psql -c 'CREATE INDEX ON crsp.dsi (date)'");
    system("psql -f crsp/make_trading_dates.sql");
}

$dsedelist = system("./wrds_update.pl crsp.dsedelist --fix-missing");
$dsedelist = $dsedelist >> 8;

if ($dsedelist) {
    system("psql -c 'ALTER TABLE crsp.dsedist ALTER permno TYPE integer;'");
    system("psql -c 'CREATE INDEX ON crsp.dsedelist (permno)'");
}

$dport = system("./wrds_update.pl crsp.dport1");
$dport = $dport >> 8;

if ($dport) {
    print "Getting erdport1\n";
    system("crsp/get_erdport.pl");
    system("psql -c 'ALTER TABLE crsp.dport1 ALTER permno TYPE integer;'");
    system("psql -f crsp/crsp_make_erdport1.sql");
    system("psql -c 'CREATE INDEX ON crsp.dport1 (permno, date)'");
}

if ($dport | $dsf | $dsi | $dsedelist) {
    system("psql -f crsp/crsp_make_rets_alt.sql");
}

$ccmxpf_linktable = system("./wrds_update.pl crsp.ccmxpf_linktable --fix-missing");
$ccmxpf_linktable = $ccmxpf_linktable >> 8;

if ($ccmxpf_linktable) {
    system("psql -c 'ALTER TABLE crsp.ccmxpf_linktable ALTER lpermno TYPE integer;'");
    system("psql -c 'ALTER TABLE crsp.ccmxpf_linktable ALTER lpermco TYPE integer;'");
    system("psql -c 'ALTER TABLE crsp.ccmxpf_linktable ALTER usedflag TYPE integer;'");
    system("psql -c 'CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)'");
    system("psql -c 'CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)'");
    system("psql -c 'CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)'");
}

$ccmxpf_lnkhist = system("./wrds_update.pl crsp.ccmxpf_lnkhist --fix-missing");
$ccmxpf_lnkhist = $ccmxpf_lnkhist >> 8;

if ($ccmxpf_lnkhist) {
    system("psql -c 'CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)'");
}

$dsedist = system("./wrds_update.pl crsp.dsedist --fix-missing");
$dsedist = $dsedist >> 8;

if ($dsedist) {
    system("psql -c 'CREATE INDEX ON crsp.dsedist (permno)'");
}

$stocknames = system("./wrds_update.pl crsp.stocknames");
$stocknames = $stocknames >> 8;

if ($stocknames) {
    system("psql -c 'ALTER TABLE crsp.stocknames ALTER permno TYPE integer'");
    system("psql -c 'ALTER TABLE crsp.stocknames ALTER permco TYPE integer'");
}

$dseexchdates = system("./wrds_update.pl crsp.stocknames");
$dseexchdates = $dseexchdates >> 8;
if ($dseexchdates) {
    system("psql -c 'ALTER TABLE crsp.dseexchdates ALTER permno TYPE integer;'");
    system("psql -c 'CREATE INDEX ON crsp.dseexchdates (permno)'");
}

# Update other data sets
system("./wrds_update.pl crsp.msp500list;");
system("./wrds_update.pl crsp.ccmxpf_lnkused --fix-missing;");
system("./wrds_update.pl crsp.fund_names --fix-missing;");

# Fix permissions.
system("psql -c 'GRANT USAGE ON SCHEMA crsp TO wrds_basic'");
system("psql -c 'GRANT SELECT ON ALL TABLES IN SCHEMA crsp TO wrds_basic'");

