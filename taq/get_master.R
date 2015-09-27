library("RPostgreSQL")
pg <- dbConnect(PostgreSQL())

dbGetQuery(pg, "
    DROP TABLE IF EXISTS taq.mast;
    CREATE TABLE taq.mast
    (
      symbol text,
      name text,
      cusip text,
      fdate date,
      shrout double precision
    )")

dbDisconnect(pg)

sas_code <- "
    libname pwd '.';

    options nosource nonotes;

    data mast;
        set taq.mast:;
    run;

    proc sql;
        CREATE TABLE pwd.mast AS
        SELECT DISTINCT symbol, name, cusip, fdate, shrout
        FROM mast;
    quit;"

#

# Use PostgreSQL's COPY function to get data into the database
cmd = paste0("echo \"", sas_code, "\" | ",
            "ssh -C $WRDS_ID@wrds.wharton.upenn.edu 'sas -stdio -noterminal' 2>/dev/null ")

system(cmd)
system("psql -c 'CREATE SCHEMA IF NOT EXISTS home'")

system("./wrds_update.pl home.mast --fix-cr")

system("psql -c 'DROP TABLE IF EXISTS taq.mast'")
system("psql -c 'ALTER TABLE home.mast SET SCHEMA taq'")
system("psql -c 'DROP SCHEMA IF EXISTS home'")
