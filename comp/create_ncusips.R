library(DBI)
suppressPackageStartupMessages(library(dplyr))
pg <- dbConnect(RPostgres::Postgres())

dbExecute(pg, "SET work_mem='5GB'")
dbExecute(pg, "SET search_path TO comp")

ccmxpf_lnkhist <-
    tbl(pg, sql("SELECT * FROM crsp.ccmxpf_lnkhist"))
stocknames <- tbl(pg, sql("SELECT * FROM crsp.stocknames"))
company <- tbl(pg, sql("SELECT * FROM comp.company"))
secm <- tbl(pg, sql("SELECT * FROM comp.secm"))

crsp_linktable <-
    ccmxpf_lnkhist %>%
    filter(linktype %in% c('LC', 'LU', 'LS')) %>%
    mutate(permno=as.integer(lpermno)) %>%
    select(gvkey, permno, linkdt, linkenddt, liid, linktype) %>%
    rename(iid = liid) %>%
    compute(indexes="permno")

gvkey_cusip <-
    secm %>%
    select(gvkey, iid, datadate, tic, cusip, conm, cik) %>%
    filter(!is.na(cik)) %>%
    mutate(cik = as.integer(cik),
           cusip = substr(cusip, 1L, 8L))

gvkey_permno <-
    gvkey_cusip %>%
    inner_join(crsp_linktable) %>%
    filter(datadate >= linkdt | is.na(linkdt),
           datadate <= linkenddt | is.na(linkenddt))

gvkey_permno_cusip_raw <-
    gvkey_permno %>%
    inner_join(stocknames %>% select(-cusip), by="permno") %>%
    filter(datadate >= st_date | is.na(st_date),
           datadate <= end_date | is.na(end_date)) %>%
    select(gvkey, datadate, iid, linktype, cusip, permno, ncusip) %>%
    compute()

gvkey_permno_cusip_raw %>%
    mutate(same_cusip = cusip == ncusip) %>%
    count(same_cusip)

same_cusip <-
    gvkey_permno_cusip_raw %>%
    filter(!is.na(ncusip)) %>%
    filter(cusip == ncusip)

diff_cusip <-
    gvkey_permno_cusip_raw %>%
    filter(!is.na(ncusip)) %>%
    filter(cusip != ncusip)

all_ncusip_matches <-
    diff_cusip %>%
    anti_join(same_cusip, by=c("gvkey", "iid", "datadate")) %>%
    union(same_cusip)

dbExecute(pg, "DROP TABLE IF EXISTS ncusips")

all_ncusips <-
    gvkey_cusip %>%
    select(gvkey, iid, datadate, cusip) %>%
    left_join(all_ncusip_matches %>% select(-cusip),
              by = c("gvkey", "iid", "datadate")) %>%
    compute(name="ncusips", temporary=FALSE, replace=FALSE,
            indexes=c("gvkey", "ncusip"))

rs <- dbExecute(pg, "ALTER TABLE ncusips OWNER TO comp")
rs <- dbExecute(pg, "GRANT SELECT ON TABLE ncusips TO comp_access")

comment <- 'Created using create_ncusips.R'
sql <- paste0("COMMENT ON TABLE comp.ncusips IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbExecute(pg, sql)
