library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "SET search_path TO ciq, public")
rs <- dbExecute(pg, "SET work_mem TO '10GB'")

ciqfininstance <- tbl(pg, "ciqfininstance")
ciqfinperiod <- tbl(pg, "ciqfinperiod")
ciqgvkeyiid <- tbl(pg, "ciqgvkeyiid")

company <- tbl(pg, sql("SELECT * FROM comp.company"))
filings <- tbl(pg, sql("SELECT * FROM edgar.filings"))
accession_numbers <- tbl(pg, sql("SELECT * FROM edgar.accession_numbers"))

ciq_data <-
    ciqfininstance %>%
    inner_join(ciqfinperiod, by = "financialperiodid") %>%
    inner_join(ciqgvkeyiid, by=c("companyid"="relatedcompanyid"))

ciq_acc_nos <-
    ciq_data %>%
    filter(!is.na(accessionnumber)) %>%
    distinct(gvkey, iid, accessionnumber) %>%
    compute()

rs <- dbExecute(pg, "DROP TABLE IF EXISTS gvkey_ciks")

gvkey_ciks <-
    ciq_acc_nos %>%
    inner_join(accession_numbers, by = "accessionnumber") %>%
    inner_join(filings, by = "file_name") %>%
    group_by(gvkey, iid, cik) %>%
    summarize(first_date = min(date_filed, na.rm = TRUE),
              last_date = max(date_filed, na.rm = TRUE)) %>%
    compute(name = "gvkey_ciks", temporary = FALSE)

rs <- dbExecute(pg, "ALTER TABLE gvkey_ciks OWNER TO ciq")
rs <- dbExecute(pg, "GRANT SELECT ON TABLE gvkey_ciks TO ciq_access")
