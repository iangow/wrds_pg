#!/usr/bin/env Rscript
library(DBI)
library(haven)
library(dplyr, warn.conflicts = FALSE)

convertToInteger <- function(vec) {
    # This is a small function that converts numeric vectors to
    # integers if doing so does not lose information
    notNA <- !is.na(vec)

    if (all(vec[notNA]==bit64::as.integer64(vec[notNA]))) {
        return(bit64::as.integer64(vec))
    } else {
        return(vec)
    }
}

fix_names <- function(df) {
  names(df) <- tolower(names(df))
  df
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

    temp <-
      read_dta(temp_file, encoding = "latin1") %>%
      fix_names() %>%
      mutate_if(is.numeric, convertToInteger)

    # Convert numeric vectors to integers if possible
    if (datatable=="votes") {
        temp <-
          temp %>%
          filter(!is.na(year)) %>%
          fix_data()
    }

    if (datatable=="proposals") {
       temp <- fix_proposals(temp)
    }

    # Delete the temporary file
    unlink(temp_file)
    return(temp)
}

fix_data <- function(df) {

    df %>%
      mutate_at(vars(votes_against,
                     abstentions_mgmt_proposals_on,
                     company_results_for_shareholder,
                     abstentions_mgmt_proposals_on,
                     company_results_against_shareho,
                     company_results_abstentions_sha,
                     irrc_issue_code), as.numeric)
}

# Get data mapping issue codes to categories in Cunat et al. (JF, 2012)

library(googlesheets)
# gs_auth()
gs <- gs_key("1RrNACT_vKo7eT_HngWPhcRGOpJ09Ss4xipdmqsc5yuA")

issue_codes <- gs_read(gs, ws = "issue_codes")

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "CREATE SCHEMA IF NOT EXISTS risk")
rs <- dbExecute(pg, "SET search_path TO risk")
rs <- dbWriteTable(pg, "issue_codes", issue_codes,
                   row.names=FALSE, overwrite=TRUE)
rs <- dbDisconnect(pg)

fix_proposals <- function(df) {
    df$issue_code <- as.integer(df$issue_code)
    df$issue_code[df$resolution=="no consulting by auditors"] <-2002L
    return(df)
}

# Now get the data from WRDS
replicate <- function(libname, datatable) {
    temp <- get_data(libname, datatable)

    pg <- dbConnect(RPostgres::Postgres())

    dbExecute(pg, paste0("SET search_path TO ", libname))

    dbWriteTable(pg, datatable, temp,
                 overwrite=TRUE, row.names=FALSE)
    dbDisconnect(pg)
}

# replicate("risk", "proposals")
# replicate("risk", "rmgovernance")
replicate("risk", "gset")
replicate("risk", "votes")
replicate("risk", "directors")

