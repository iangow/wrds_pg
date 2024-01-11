#!/usr/bin/env python3
from wrds2pg import wrds_update, run_file_sql, make_engine, process_sql
import datetime
from wrds2pg import get_pg_tables, get_cols
import re
    
engine = make_engine()

now = datetime.datetime.now()
avail_years = range(2000, now.year+1)

updated = wrds_update("rpa_entity_mappings", "rpa")

def update_equities(year):
    updated = wrds_update("rpa_djpr_equities_" + str(year), "rpa",
                          col_types = {"timestamp_utc": "timestamptz",
                                       "rpa_time_utc": "text",
                                       "event_start_date_utc": "timestamptz", 
                                       "event_end_date_utc": "timestamptz", 
                                       "reporting_start_date_utc": "timestamptz",
                                       "reporting_end_date_utc": "timestamptz"})
    if updated:
        process_sql("CREATE INDEX ON rpa.rpa_djpr_equities_" + str(year) + " (rp_entity_id, timestamp_utc)",
                    engine)
    return updated

updated = [ update_equities(year) for year in avail_years]

if any(updated):
    
    def select(cols):
        sql = 'SELECT "' + '", "'.join(cols) + '"'
        return sql
    
    def get_sql(table, cols):
        return(select(cols) + f"\nFROM {table}\n")
    
    tables = [key for key in get_pg_tables("rpa", engine, keys=True)
              if re.search("rpa_djpr_equities", key)]
    cols = get_cols(max(tables), "rpa", engine)
    sql_parts = [get_sql(table, cols) for table in tables]
    
    sql =  "DROP VIEW IF EXISTS rpa.rpa_djpr_equities CASCADE;"
    sql += "CREATE VIEW rpa.rpa_djpr_equities AS\n"
    sql += "UNION ALL\n".join(sql_parts) + ";"
    sql += "ALTER VIEW rpa.rpa_djpr_equities OWNER TO rpa;"
    sql += "GRANT SELECT ON rpa.rpa_djpr_equities TO rpa_access;"
    
    process_sql(sql, engine)
