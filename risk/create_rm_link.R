library(dplyr)
library(RPostgreSQL)

pg <- src_postgres()

rmdirectors <-
    tbl(pg, sql("SELECT * FROM risk.rmdirectors")) %>%
    filter(last_name ~ '[A-Za-z]') %>%
    mutate(cusip = if_else(cusip ~ '^0+$', NA_character_, cusip)) %>%
    mutate(cusip = substr(cusip, 1L, 8L))

stocknames <-
    tbl(pg, sql("SELECT * FROM crsp.stocknames"))

cusip_permnos <-
    stocknames %>%
    select(ncusip, permno) %>%
    distinct() %>%
    rename(cusip = ncusip)

library(googlesheets)

gs <- gs_key("1quUwIqc8jsxpsSgMrO2Ig9UBpaFrdQP357bx1u0i-Og")
hand_matches <-
    gs_read(gs, ws = "rm_ids") %>%
    select(company_id, permno)

link_table <-
    rmdirectors %>%
    inner_join(cusip_permnos) %>%
    select(company_id, permno) %>%
    filter(!is.na(permno)) %>%
    union(hand_matches, copy=TRUE) %>%
    compute(name="rm_link", temporary=FALSE,
            indexes=c("company_id", "permno"))

dbGetQuery(pg$con, "GRANT SELECT ON rm_link TO wrds")
dbGetQuery(pg$con, "DROP TABLE IF EXISTS risk.rm_link")
dbGetQuery(pg$con, "ALTER TABLE rm_link SET SCHEMA risk")

comment <- 'Created using create_rm_link.R'
sql <- paste0("COMMENT ON TABLE risk.rm_link IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbGetQuery(pg$con, sql)

# linked <-
#     rmdirectors %>%
#     left_join(link_table) %>%
#     mutate(has_permno = !is.na(permno))
#
# linked %>%
#     count(has_permno)

# library(readr)
# linked %>%
#     filter(!has_permno) %>%
#     select(company_id, cusip, ticker, name) %>%
#     distinct() %>%
#     collect() %>%
#     write_csv("~/Google Drive/director_photo/rm_ids.csv")

