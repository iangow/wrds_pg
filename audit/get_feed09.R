# Set up functions ----
convertToBoolean <- function(table, var) {
    library("RPostgreSQL")
    pg <- dbConnect(PostgreSQL())
    sql <- paste0("ALTER TABLE ", table," ALTER COLUMN ",
                  var, " TYPE boolean USING ", var, "=1")
    dbGetQuery(pg, sql)
    dbDisconnect(pg)
}

convertToDouble <- function(table, var) {
    library("RPostgreSQL")
    pg <- dbConnect(PostgreSQL())
    sql <- paste0("ALTER TABLE ", table, " ALTER COLUMN ",
                  var, " TYPE float8 USING ", var, "::float8")
    dbGetQuery(pg, sql)
    dbDisconnect(pg)
}

convertToInteger <- function(table, var) {
    library("RPostgreSQL")
    pg <- dbConnect(PostgreSQL())
    sql <- paste0("ALTER TABLE ", table, " ALTER COLUMN ",
                  var, " TYPE integer USING ", var)
    dbGetQuery(pg, sql)
    dbDisconnect(pg)
}

# Get other feed09 tables ----
library("RPostgreSQL")
pg <- dbConnect(PostgreSQL())

dbGetQuery(pg, "
           DROP TABLE IF EXISTS audit.feed09cat;
           DROP TABLE IF EXISTS audit.feed09tocat")

dbDisconnect(pg)

system('perl ./wrds_update.pl audit.feed09cat')
system('perl ./wrds_update.pl audit.feed09tocat')
system('perl ./wrds_update.pl audit.feed09period')

convertToInteger("audit.feed09cat", "res_category_fkey")
convertToInteger("audit.feed09tocat", "res_notify_key")
convertToInteger("audit.feed09tocat", "res_category_fkey")
convertToInteger("audit.feed09period", "res_notify_key")
convertToDouble("audit.feed09period", "res_period_aud_fkey")
convertToInteger("audit.feed09period", "res_period_aud_fkey")
