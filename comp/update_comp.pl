#!/usr/bin/env perl

$anncomp = system("./wrds_update.pl comp.anncomp");
$anncomp = $anncomp >> 8;
if ($anncomp) {
    system("psql -c 'CREATE INDEX ON comp.anncomp (gvkey)'");
}

$adsprate = system("./wrds_update.pl comp.adsprate");
$adsprate = $adsprate >> 8;
if ($adsprate) {
    system("psql -c 'CREATE INDEX ON comp.adsprate (gvkey, datadate)'");
}

$co_hgic = system("./wrds_update.pl comp.co_hgic");
$co_hgic = $co_hgic >> 8;
if ($co_hgic) {
    system("psql -c 'CREATE INDEX ON comp.co_hgic (gvkey)'");
}

$co_ifndq = system("./wrds_update.pl comp.co_ifndq");
$co_ifndq = $co_ifndq >> 8;
if ($co_ifndq) {
    system("psql -c 'CREATE INDEX ON comp.co_ifndq (gvkey, datadate)'");
}

$company = system("./wrds_update.pl comp.company");
$company = $company >> 8;
if ($company) {
    system("psql -c 'CREATE INDEX ON comp.company (gvkey)'");
}


$idx_ann = system("./wrds_update.pl comp.idx_ann");
$idx_ann = $idx_ann >> 8;
if ($idx_ann) {
    system("psql -c 'CREATE INDEX ON comp.idx_ann (datadate)'");
}

$idx_index = system("./wrds_update.pl comp.idx_index");

$io_qbuysell = system("./wrds_update.pl comp.io_qbuysell");
$io_qbuysell = $io_qbuysell >> 8;
if ($io_qbuysell) {
    system("psql -c 'CREATE INDEX ON comp.io_qbuysell (gvkey, datadate)'");
}

$names = system("./wrds_update.pl comp.names");
$names = $names >> 8;
if ($names) {
    system("psql -c 'CREATE INDEX ON comp.names (gvkey)'");
}

$secm = system("./wrds_update.pl comp.secm");
$secm = $secm >> 8;
if ($secm) {
    system("psql -c 'CREATE INDEX ON comp.secm (gvkey, datadate)'");
}


$secd = system("./wrds_update.pl comp.secd");
$secd = $secd >> 8;
if ($secd) {
    system("psql -c 'CREATE INDEX ON comp.secd (gvkey, datadate)'");
}


$wrds_segmerged = system("./wrds_update.pl comp.wrds_segmerged");
$wrds_segmerged = $wrds_segmerged >> 8;
if ($wrds_segmerged) {
    system("psql -c 'CREATE INDEX ON comp.wrds_segmerged (gvkey, datadate)'");
}

$spind_mth = system("./wrds_update.pl comp.spind_mth");
$spind_mth = $spind_mth >> 8;
if ($spind_mth) {
    system("psql -c 'CREATE INDEX ON comp.spind_mth (gvkey, datadate)'");
}

$funda = system("./wrds_update.pl comp.funda --fix-missing");
$funda = $funda >> 8;
if ($funda) {
    system("psql -c 'CREATE INDEX ON comp.funda (gvkey, datadate)'");
}

$fundq = system("./wrds_update.pl comp.fundq --fix-missing");
$fundq = $fundq >> 8;
if ($fundq) {
    system("psql -c 'CREATE INDEX ON comp.fundq (gvkey, datadate)'");
}

$g_sec_divid = system("./wrds_update.pl comp.g_sec_divid");
$g_sec_divid = $g_sec_divid >> 8;
if ($g_sec_divid) {
    system("psql -c 'CREATE INDEX ON comp.g_sec_divid (gvkey, datadate)'");
}

$idxcst_his = system("./wrds_update.pl comp.idxcst_his --rename=from=fromdt");
$idxcst_his = $idxcst_his >> 8;

system("psql < comp/create_ciks.sql");

if ($secm | $company) {
    system("psql < comp/create_ciks.sql");
}

system("psql -c 'GRANT USAGE ON SCHEMA comp TO wrds'");
system("psql -c 'GRANT SELECT ON ALL TABLES IN SCHEMA comp TO wrds'");


