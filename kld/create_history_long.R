library(dplyr, warn.conflicts = FALSE)
library(DBI)
library(tidyr)

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "SET search_path TO kld")
history <- tbl(pg, "history")

hist_long <-
  history %>%
  collect() %>%
  mutate(year = as.integer(year),
         ticker = if_else(ticker == "#N/A", NA_character_, ticker),
         cusip = if_else(cusip == "0", NA_character_, cusip)) %>%
  filter(!is.na(ticker) | !is.na(cusip) | !is.na(legacy_companyid)) %>%
  pivot_longer(cols = matches("_(str|con)_"),
               names_to = c("category", "type", "label"),
               names_sep = "_")

dbWriteTable(pg, name = "history_long", hist_long,
             row.names = FALSE, overwrite = TRUE)

rs <- dbExecute(pg, "ALTER TABLE history_long OWNER TO kld")
rs <- dbExecute(pg, "GRANT SELECT ON history_long TO kld_access")

dbDisconnect(pg)
