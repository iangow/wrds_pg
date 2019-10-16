library(haven)
library(DBI)
library(dplyr, warn.conflicts = FALSE)

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

fix_names <- function(df) {
  names(df) <- tolower(names(df))
  df
}

get_iclink <- function() {

  sas_code <- "

    %include \"/wrdslin/ibes/samples/iclink.sas\";

    proc export data=home.iclink
      outfile=\"data.dta\"
      dbms=stata replace;
    run;"

  temp_file <- tempfile()
  # This command calls SAS on the remote server.
  # -C means "compress output" ... this seems to have an impact even though we're
  # using gzip for compression of the CSV file spat out by SAS after it's
  # been transferred to the local computer (trial and error suggested this was
  # the most efficient approach).
  # -stdio means that SAS will take input from STDIN and output to STDOUT
  sas_command <- paste("ssh -C $WRDS_ID@wrds-cloud.wharton.upenn.edu ",
                       "'qsas -stdio -noterminal; cat data.dta' > ",
                       temp_file, " 2>/dev/null")

  # The following pipes the SAS code to the SAS command. The "intern=TRUE"
  # means that we can capture the output in an R variable.
  system(paste("echo '", sas_code, "' |", sas_command), intern=FALSE)

  temp <-
    read_dta(temp_file) %>%
    mutate_if(is.numeric, convertToInteger) %>%
    fix_names()

  # Delete the temporary file
  unlink(temp_file)
  return(temp)
}

# Now get the data from WRDS
system.time(iclink <- get_iclink())

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO ibes")

rs <- dbWriteTable(pg, "iclink", iclink, overwrite=TRUE, row.names=FALSE)

rs <- dbExecute(pg, "CREATE INDEX ON iclink (ticker)")
rs <- dbExecute(pg, "CREATE INDEX ON iclink (permno)")
rs <- dbExecute(pg, "ALTER TABLE iclink OWNER TO ibes")
rs <- dbExecute(pg, "GRANT SELECT ON iclink TO ibes_access")
rs <- dbDisconnect(pg)

pg_comment <- function(table, comment) {
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())
    sql <- paste0("COMMENT ON TABLE ", table, " IS '",
                  comment, " ON ", Sys.Date() , "'")
    rs <- dbGetQuery(pg, sql)
    dbDisconnect(pg)
}

pg_comment("ibes.iclink", "Created using ibes/get_iclink.R")
