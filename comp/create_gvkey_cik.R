library(dplyr)
library(RPostgreSQL)

pg <- src_postgres()

dbGetQuery(pg$con, "SET work_mem='8GB'")

ciqfininstance <-
    tbl(pg, sql("SELECT * FROM ciq.ciqfininstance"))

ciqfinperiod <-
    tbl(pg, sql("SELECT * FROM ciq.ciqfinperiod"))

ciqgvkeyiid <-
    tbl(pg, sql("SELECT * FROM ciq.ciqgvkeyiid"))

accession_numbers <-
    tbl(pg, sql("SELECT * FROM filings.accession_numbers"))

filings <-
    tbl(pg, sql("SELECT * FROM filings.filings"))

gvkeys <-
    ciqgvkeyiid %>%
    select(gvkey, iid, relatedcompanyid)

dbGetQuery(pg$con, "DROP TABLE IF EXISTS gvkey_cik")

gvkey_ciks <-
    ciqfininstance %>%
    inner_join(accession_numbers) %>%
    inner_join(ciqfinperiod) %>%
    inner_join(gvkeys, by=c("companyid"="relatedcompanyid")) %>%
    select(gvkey, iid, file_name, periodenddate) %>%
    distinct() %>%
    compute(name = "gvkey_cik", temporary = FALSE,
            indexes = c("file_name", "gvkey"), overwrite=TRUE)

dbGetQuery(pg$con, "GRANT SELECT ON gvkey_cik TO wrds")
dbGetQuery(pg$con, "DROP TABLE IF EXISTS comp.gvkey_cik")
dbGetQuery(pg$con, "ALTER TABLE gvkey_cik SET SCHEMA comp")

comment <- 'Created using create_gvkey_cik.R'
sql <- paste0("COMMENT ON TABLE comp.gvkey_cik IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbGetQuery(pg$con, sql)

