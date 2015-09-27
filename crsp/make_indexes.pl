# Convert date fields from integers to dates
foreach $key (keys %hrec) { 
    $value = $hrec{$key};
}

# Create indices for performance
if ($table_name eq "ccmxpf_linktable") {
    $sql = "CREATE INDEX $table_name" ."_lpermno_idx ON $table_name (lpermno);";
    print "$sql\n";
    $dbh->do($sql);
    $sql = "CREATE INDEX $table_name" ."_main_idx ON $table_name (gvkey);";
    print "$sql\n";
    $dbh->do($sql);
    $dbh->do("CLUSTER $table_name USING $table_name" ."_main_idx");
    $index=0;
}

if ($table_name eq "dsf" || $table_name eq "erdport1") {
    $sql = "ALTER TABLE $table_name ADD PRIMARY KEY (permno, date);";
    print "$sql\n";
    $dbh->do($sql);
    
    $dbh->do("CLUSTER $table_name USING $table_name" . "_pkey");
    
    $sql = "CREATE INDEX $table_name" ."_date_idx ON $table_name (date);";
    print "$sql\n";
    $dbh->do($sql);

    $index=0;
}

if ($table_name eq "dsi") {
    $sql = "ALTER TABLE $table_name ADD PRIMARY KEY (date);";
    print "$sql\n";
    $dbh->do($sql);
    $dbh->do("CLUSTER $table_name USING $table_name" . "_pkey");
    $index=0;
  $cmd = "psql -d crsp < ~/Dropbox/WRDS/make_trading_dates_pg.sql";
  $result = system($cmd);
  print "$result";
  
}

if ($table_name eq "company" and $db_schema eq "crsp") {
    $sql = "ALTER TABLE $table_name ADD PRIMARY KEY (permno);";
    print "$sql\n";
    $dbh->do($sql);
    $dbh->do("CLUSTER $table_name USING $table_name" . "_pkey");
    $index=0;
}

if ($table_name eq "fundq") {
    $dbh->do("CREATE VIEW compq.fundq AS SELECT * FROM comp.fundq");
}

if ($table_name eq "s34") {
    $dbh->do("ALTER TABLE tfn.s34 ALTER mgrno TYPE integer");
}


if ($table_name eq "stocknames") {
    $dbh->do("ALTER TABLE crsp.stocknames ALTER COLUMN permno TYPE bigint");
    $dbh->do("ALTER TABLE crsp.stocknames ALTER COLUMN permco TYPE bigint");
}


if ($has_date ==1 and $has_permno==1) {
    $index_on = "(PERMNO, DATE)";
    $index +=1;
} elsif ($has_date==1) {
    $index_on = "(DATE)";
    $index +=1;
} elsif ($has_permno==1) {
    $index_on = "(PERMNO)";
    $index +=1;
}

if ($has_datadate ==1 & $has_gvkey==1) {
    $index_on = "(gvkey, datadate)";
    $index +=1;
} elsif ($has_datadate==1) {
    $index_on = "(datadate)";
    $index +=1;
} elsif ($has_gvkey==1) {
    $index_on = "(gvkey)";
    $index +=1;
}

if ($index) {
    $sql = "CREATE INDEX ON $table_name $index_on;";
    print "$sql\n";
    $dbh->do($sql);
}

if ($table_name eq "secm") {
     $sql = "DROP TABLE IF EXISTS comp.cusips;";
    $sql .= "CREATE TABLE comp.cusips AS SELECT DISTINCT gvkey, cusip FROM comp.secm;";
    print "$sql\n";
    $dbh->do($sql);   
}

  
    if ($table_name eq "ccmxpf_linktable") {
        $dbh->do("ALTER TABLE crsp.ccmxpf_lnkused ALTER usedflag TYPE integer");
        $dbh->do("ALTER TABLE crsp.ccmxpf_lnkused ALTER apermno TYPE integer");
        $dbh->do("ALTER TABLE crsp.ccmxpf_lnkused ALTER upermno TYPE integer");
        $dbh->do("ALTER TABLE crsp.ccmxpf_lnkused ALTER upermco TYPE integer");
        $dbh->do("ALTER TABLE crsp.ccmxpf_lnkused ALTER ulinkid TYPE integer");
        $dbh->do("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermco TYPE integer");
    }