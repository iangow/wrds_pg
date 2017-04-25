library(RPostgreSQL)
suppressPackageStartupMessages(library(dplyr))
pg <- src_postgres()

dbGetQuery(pg$con, "SET work_mem='5GB'")

ncusips <-
    tbl(pg, sql("SELECT * FROM comp.ncusips"))
stocknames <- tbl(pg, sql("SELECT * FROM crsp.stocknames"))
company <- tbl(pg, sql("SELECT * FROM comp.company"))

ambiguous_cases <-
    ncusips %>%
    select(gvkey, permno, datadate, ncusip) %>%
    distinct() %>%
    group_by(ncusip, datadate) %>%
    filter(n() > 1) %>%
    inner_join(
        company %>%
            select(gvkey, conm)) %>%
    inner_join(
        stocknames %>%
            select(permno, ncusip, comnam)) %>%
    arrange(ncusip) %>%
    ungroup() %>%
    compute()

ambiguous_cases %>%
    count()

