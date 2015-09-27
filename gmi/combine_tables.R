library(RPostgreSQL)

# Skip 2005-2007, as included in "takeoverdefenses"
tables <- as.list(paste0("takeoverdefenses", c("", 2008:2013)))
pg <- dbConnect(PostgreSQL())

db_fields <- dbListFields(pg, c("gmi", "takeoverdefenses"))

# Drop a problematic field that seems not to have any data in it.
db_fields <- setdiff(db_fields, "tddirectorremovalvotepercent")
fields <- paste0(db_fields, collapse=", ")

table_query <- function(table) {
    paste0("SELECT ", fields, "\nFROM gmi.", table)
}

sql <- paste0("DROP TABLE gmi.takeoverdefenses_all;\n",
              "CREATE TABLE gmi.takeoverdefenses_all AS\n", 
              paste0(lapply(tables, table_query), collapse="\nUNION\n"))

rs <- dbGetQuery(pg, sql)

sql <- paste("CREATE INDEX ON gmi.takeoverdefenses_all (ticker);",
             "CREATE INDEX ON gmi.takeoverdefenses_all (ticker, year);", 
             sep="\n")
rs <- dbGetQuery(pg, sql)
# cat(sql)
