library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres())

dbExecute(pg, "SET work_mem='8GB'")
dbExecute(pg, "SET search_path TO comp, ciq")

ciqfininstance <- tbl(pg, "ciqfininstance")
ciqfinperiod <- tbl(pg, "ciqfinperiod")
ciqgvkeyiid <- tbl(pg, "ciqgvkeyiid")

accession_numbers <- tbl(pg, sql("SELECT * FROM edgar.accession_numbers"))
filings <- tbl(pg, sql("SELECT * FROM edgar.filings"))

gvkeys <-
    ciqgvkeyiid %>%
    select(gvkey, iid, relatedcompanyid)

dbExecute(pg, "DROP TABLE IF EXISTS gvkey_cik")

gvkey_ciks <-
    ciqfininstance %>%
    inner_join(accession_numbers) %>%
    inner_join(ciqfinperiod) %>%
    inner_join(gvkeys, by=c("companyid"="relatedcompanyid")) %>%
    select(gvkey, iid, file_name, periodenddate) %>%
    distinct() %>%
    compute(name = "gvkey_cik", temporary = FALSE,
            indexes = c("file_name", "gvkey"), overwrite=TRUE)

dbExecute(pg, "ALTER TABLE gvkey_cik OWNER TO comp")
dbExecute(pg, "GRANT SELECT ON gvkey_cik TO comp_access")

comment <- 'Created using create_gvkey_cik.R'
sql <- paste0("COMMENT ON TABLE gvkey_cik IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbExecute(pg, sql)
