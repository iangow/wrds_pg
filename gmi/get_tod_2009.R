# Download SAS file and read into R
cmd <- "scp $WRDS_ID@wrds.wharton.upenn.edu"
cmd <- paste0(cmd, ":/wrds/gmiratings/sasdata/takeoverdefenses2009.sas7bdat .")
system(cmd)
library(haven)
takeoverdefenses2009 <- as.data.frame(read_sas("takeoverdefenses2009.sas7bdat"))
names(takeoverdefenses2009) <- tolower(names(takeoverdefenses2009))
unlink("takeoverdefenses2009.sas7bdat")

# Push data to the database
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
    
rs <- dbWriteTable(pg, c("gmi", "takeoverdefenses2009"), takeoverdefenses2009,
                   overwrite=TRUE, row.names=FALSE)
dbDisconnect(pg)




