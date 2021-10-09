library(dplyr)
library(DBI)

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "SET search_path TO risk")

rmdirectors <-
    tbl(pg, sql("SELECT * FROM risk.rmdirectors")) %>%
    filter(last_name %~% '[A-Za-z]') %>%
    mutate(cusip = if_else(cusip %~% '^0+$', NA_character_, cusip)) %>%
    mutate(cusip = substr(cusip, 1L, 8L))

stocknames <-
    tbl(pg, sql("SELECT * FROM crsp.stocknames"))

cusip_permnos <-
    stocknames %>%
    select(ncusip, permno) %>%
    distinct() %>%
    rename(cusip = ncusip)

library(googlesheets4)

gs <- as_sheets_id("1quUwIqc8jsxpsSgMrO2Ig9UBpaFrdQP357bx1u0i-Og")

hand_matches <-
    read_sheet(gs) %>%
    select(company_id, permno)

dbExecute(pg, "DROP TABLE IF EXISTS rm_link")

link_table <-
    rmdirectors %>%
    inner_join(cusip_permnos) %>%
    select(company_id, permno) %>%
    filter(!is.na(permno)) %>%
    union(hand_matches, copy=TRUE) %>%
    compute(name="rm_link", temporary=FALSE,
            indexes=c("company_id", "permno"))

rs <- dbExecute(pg, "GRANT SELECT ON rm_link TO risk_access")
rs <- dbExecute(pg, "ALTER TABLE rm_link OWNER TO risk")

comment <- 'Created using create_rm_link.R'
sql <- paste0("COMMENT ON TABLE risk.rm_link IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbExecute(pg, sql)
