# Get data mapping issue codes to categories in Cunat et al. (JF, 2012)
# gs_auth()
library(googlesheets4)
library(DBI)

gs <- as_sheets_id("1RrNACT_vKo7eT_HngWPhcRGOpJ09Ss4xipdmqsc5yuA")

issue_codes <- read_sheet(gs, sheet = "issue_codes")

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "CREATE SCHEMA IF NOT EXISTS risk")
rs <- dbExecute(pg, "SET search_path TO risk")
rs <- dbWriteTable(pg, "issue_codes", issue_codes,
                   row.names=FALSE, overwrite=TRUE)
rs <- dbDisconnect(pg)
