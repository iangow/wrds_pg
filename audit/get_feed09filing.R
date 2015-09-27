library("RPostgreSQL")
pg <- dbConnect(PostgreSQL())
dbname <- Sys.getenv("PGDATABASE")

dbGetQuery(pg, "
    DROP TABLE IF EXISTS audit.disclosure_text;
    CREATE TABLE audit.disclosure_text
           (res_notify_key integer, disclosure_text text);")

dbGetQuery(pg, "
    DROP TABLE IF EXISTS audit.feed09filing ;
    CREATE TABLE audit.feed09filing
           (res_notify_key integer,
            res_accounting boolean, res_fraud boolean, res_cler_err boolean,
            res_adverse boolean, res_improves boolean,
            res_begin_date date, res_end_date date,
            res_aud_letter text, res_other boolean,
            res_sec_invest boolean, res_board_app text,
            ftp_file_fkey text, form_fkey text,
            file_date date, file_accepted text,
            file_size text, http_name_html text,
            http_name_text text, company_fkey text);")

dbDisconnect(pg)

sas_code <- "
    libname pwd '/sastemp6';

    options nosource nonotes;

    proc sql;
        CREATE TABLE pwd.auditnonreli AS
        SELECT res_notify_key, res_accounting, res_fraud, res_cler_err,
            res_adverse, res_improves, res_begin_date,
            res_end_date,
            res_aud_letter, res_other, res_sec_invest, res_board_app,
            ftp_file_fkey, form_fkey,
            file_date, file_accepted, file_size, http_name_html,
            http_name_text, company_fkey
        FROM audit.auditnonreli;
    quit;

    proc export data=pwd.auditnonreli
            outfile=stdout dbms=csv;
    run;"

# Use PostgreSQL's COPY function to get data into the database
cmd = paste0("echo \"", sas_code, "\" | ",
            "ssh -C $WRDS_ID@wrds.wharton.upenn.edu 'sas -stdio -noterminal' 2>/dev/null | ",
            "psql -d ", dbname,
            " -c \"COPY audit.feed09filing FROM STDIN CSV HEADER ENCODING 'latin1' \"")

system(cmd)

library("RPostgreSQL")
pg <- dbConnect(PostgreSQL())

dbGetQuery(pg, "
    ALTER TABLE audit.feed09filing
        ALTER file_accepted TYPE timestamp
        USING regexp_replace(file_accepted,  '(\\d{2}[A-Z]{3}\\d{4}):', '\\1 ' )::timestamp")
dbDisconnect(pg)

sas_code <- "
    libname pwd '/sastemp6';

    options nosource nonotes;

    proc sql;
        CREATE TABLE pwd.disclosure_text AS
        SELECT res_filing_key, disclosure_text
        FROM audit.feed09filing;
    quit;

    proc export data=pwd.disclosure_text
            outfile=stdout dbms=csv;
    run;"

# Use PostgreSQL's COPY function to get data into the database
cmd = paste0("echo \"", sas_code, "\" | ",
            "ssh -C $WRDS_ID@wrds.wharton.upenn.edu 'sas -stdio -noterminal' 2>/dev/null | ",
            "psql -d ", dbname,
            " -c \"COPY audit.disclosure_text FROM STDIN CSV HEADER ENCODING 'latin1' \"")

system(cmd)

# Merge tables ----
pg <- dbConnect(PostgreSQL())
dbGetQuery(pg,"
    ALTER TABLE audit.feed09filing ADD COLUMN disclosure_text text;

    UPDATE audit.feed09filing AS a SET disclosure_text=b.disclosure_text
           FROM audit.disclosure_text AS b
           WHERE a.res_notify_key=b.res_notify_key;

    DROP TABLE audit.disclosure_text;")
dbDisconnect(pg)
