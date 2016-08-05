#!/usr/bin/env Rscript 
convertToInteger <- function(vec) {
    # This is a small function that converts numeric vectors to
    # integers if doing so does not lose information
    notNA <- !is.na(vec)

    if (all(vec[notNA]==as.integer(vec[notNA]))) {
        return(as.integer(vec))
    } else {
        return(vec)
    }
}

get_data <- function(libname, datatable) {

    sas_code <- paste("
    proc export data=", libname, ".", datatable,"
      outfile=\"data.dta\"
      dbms=stata replace;
    run;", sep="")

    temp_file <- tempfile()
    # This command calls SAS on the remote server.
    # -C means "compress output" ... this seems to have an impact even though we're
    # using gzip for compression of the CSV file spat out by SAS after it's
    # been transferred to the local computer (trial and error suggested this was
    # the most efficient approach).
    # -stdio means that SAS will take input from STDIN and output to STDOUT
    wrds_id <- Sys.getenv("WRDS_ID")
    sas_command <- paste0("ssh -C ", wrds_id, "@wrds-cloud.wharton.upenn.edu ",
                         "'qsas -stdio -noterminal; cat data.dta' > ",
                         temp_file)

    # The following pipes the SAS code to the SAS command. The "intern=TRUE"
    # means that we can capture the output in an R variable.
    system(paste("echo '", sas_code, "' |", sas_command), intern=FALSE)
    library(foreign)
    temp <- read.dta(temp_file)

    # Convert numeric vectors to integers if possible
    for (i in names(temp)) {
        if(is.numeric(temp[,i])) { temp[,i] <- convertToInteger(temp[,i]) }
    }
    if (datatable=="votes") {
        temp <- fix_data(temp)
    }

    if (datatable=="proposals") {
       temp <- fix_proposals(temp)
    }

    # Delete the temporary file
    unlink(temp_file)
    return(temp)
}

fix_data <- function(df) {
    df$votes_for <- as.numeric(df$votes_for)
    df$votes_against <- as.numeric(df$votes_against)
    df$abstentions__mgmt_proposals_on <-
        as.numeric(df$abstentions__mgmt_proposals_on)
    df$company_results_for__shareholder <-
        as.numeric(df$company_results_for__shareholder)
    df$company_results_against__shareho <-
        as.numeric(df$company_results_against__shareho)
    df$company_results_abstentions__sha <-
        as.numeric(df$company_results_abstentions__sha)
    df$irrc_issue_code <- as.integer(df$irrc_issue_code)
    return(df)
}

# Get data mapping issue codes to categories in Cunat et al. (JF, 2012)
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

require(RCurl)
url <- paste("https://docs.google.com/spreadsheet/pub?",
             "key=0AvP4wvS7Nk-QdFAyX0Q1NXJ4a3ZZM3BKUUZnd0lxX3c",
             "&single=true&gid=0&output=csv",sep="")
csv_file <- getURL(url, verbose=FALSE)
issue_codes <- read.csv(textConnection(csv_file), stringsAsFactors=FALSE)
rs <- dbGetQuery(pg, "CREATE SCHEMA IF NOT EXISTS risk")
rs <- dbWriteTable(pg, c("risk", "issue_codes"), issue_codes,
                   row.names=FALSE, overwrite=TRUE)
rs <- dbDisconnect(pg)

fix_proposals <- function(df) {
    df$issue_code <- as.integer(df$issue_code)
    df$issue_code[df$resolution=="no consulting by auditors"] <-  2002L
    return(df)
}



# Now get the data from WRDS
replicate <- function(libname, datatable) {
    temp <- get_data(libname, datatable)
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())

    dbWriteTable(pg, c(libname, datatable), temp,
                 overwrite=TRUE, row.names=FALSE)
}

replicate("risk", "proposals")
replicate("risk", "gset")
replicate("risk", "rmgovernance")
# replicate("risk", "votes")

system.time(temp <- get_data("risk", "proposals"))
system.time(temp <- get_data("risk", "rmgovernance"))
# system.time(temp <- get_data("risk", "votes"))
