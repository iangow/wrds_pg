# Get corrected data on board-related activism from Google Sheets document ----
library(googlesheets4)
library(DBI)

gs <- as_sheets_id("0AuGYuDecQAVTdEc5WmhEWVY1ZWF1cjlxVFJEaHRzUFE")

manual_names <- read_sheet(gs)

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "SET search_path TO risk")

rs <- dbWriteTable(pg, "manual_names", manual_names,
                   overwrite=TRUE, row.names=FALSE)

rs <- dbExecute(pg, "ALTER TABLE manual_names OWNER TO risk")
rs <- dbExecute(pg, "GRANT SELECT ON TABLE manual_names TO risk_access")
sql <- paste("
  COMMENT ON TABLE manual_names IS
    'CREATED USING import_manual_names ON ", Sys.time() , "';", sep="")
rs <- dbExecute(pg, sql)
dbDisconnect(pg)
