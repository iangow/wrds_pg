# This code creates cik-gvkey link table using Capital IQ for executives.ciks matches.
# It uses manually verified cik-gvkey links created by analyze_cik_gvkey.R
library(dplyr, warn.conflicts = TRUE)
library(RPostgreSQL)

# Download data ----
pg <- dbConnect(PostgreSQL())
dbGetQuery(pg, "SET search_path='ciq'")

wrds_cik <- tbl(pg, "wrds_cik")
wrds_gvkey <- tbl(pg, "wrds_gvkey")

# Equilar fy_end range useful for reconciling name changes
start <-
    wrds_cik %>%
    summarize(start = min(startdate, na.rm=TRUE)) %>%
    pull()

end <-
    wrds_cik %>%
    summarize(end = max(enddate, na.rm=TRUE)) %>%
    pull()

# Capital IQ cik
wrds_ciks <-
    wrds_cik %>%
    group_by(companyid, cik) %>%
    mutate(startdate = coalesce(startdate, start),
           enddate = coalesce(enddate, end)) %>%
    summarize(startdate = min(startdate, na.rm = TRUE),
              enddate = max(enddate, na.rm = TRUE)) %>%
    mutate(cik = as.integer(cik)) %>%
    compute()

wrds_ciks_matched <-
    wrds_ciks %>%
    select(companyid, cik, startdate, enddate) %>%
    distinct() %>%
    compute()

# Capital IQ gvkey
wrds_gvkeys <-
    wrds_gvkey %>%
    group_by(companyid, gvkey) %>%
    mutate(startdate = coalesce(startdate, start),
           enddate = coalesce(enddate, end)) %>%
    summarize(startdate = min(startdate, na.rm = TRUE),
              enddate = max(enddate, na.rm = TRUE)) %>%
    compute()

dbGetQuery(pg, "DROP TABLE IF EXISTS gvkey_cik")

cik_gvkey_wrds <-
    wrds_ciks_matched %>%
    inner_join(wrds_gvkeys,
              by = "companyid",
              suffix = c("_cik", "_gvkey")) %>%
    group_by(cik, gvkey) %>%
    filter(!(enddate_gvkey < startdate_cik | enddate_cik < startdate_gvkey)) %>%
    # cik--gvkey link period
    mutate(first_date = greatest(startdate_cik, startdate_gvkey),
           last_date = least(enddate_cik, enddate_gvkey)) %>%
    select(gvkey, cik, first_date, last_date) %>%
    distinct() %>%
    compute(name = "gvkey_cik", temporary = FALSE)

dbGetQuery(pg, "ALTER TABLE gvkey_cik OWNER TO ciq")
dbGetQuery(pg, "GRANT SELECT ON TABLE gvkey_cik TO ciq_access")

comment <- 'CREATED USING ciq/create_gvkey_cik.R'
sql <- paste0("COMMENT ON TABLE gvkey_cik IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbGetQuery(pg, sql)

dbDisconnect(pg)


