from db2pq import wrds_update_pg
import datetime

now = datetime.datetime.now()
avail_years = range(2000, now.year+1)

wrds_update_pg("rpa_entity_mappings", "ravenpack_common")

def update_equities(year):
    updated = wrds_update_pg("rpa_djpr_equities_" + str(year), "ravenpack_dj",
                             col_types = {"timestamp_utc": "timestamptz",
                                          "rpa_time_utc": "string",
                                          "event_start_date_utc": "timestamptz", 
                                          "event_end_date_utc": "timestamptz", 
                                          "reporting_start_date_utc": "timestamptz",
                                          "reporting_end_date_utc": "timestamptz"})
    return updated

updated = [ update_equities(year) for year in avail_years]