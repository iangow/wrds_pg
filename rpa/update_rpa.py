#!/usr/bin/env python3
from sqlalchemy import MetaData
from wrds2pg import wrds_update, run_file_sql, make_engine
import datetime

engine = make_engine()

now = datetime.datetime.now()
avail_years = range(2000, now.year+1)

updated = wrds_update("rpa_entity_mappings", "rpa")

def update_equities(year):
    updated = wrds_update("rpa_djpr_equities_" + str(year), "rpa",
                          col_types = {"timestamp_utc": "timestamp", 
                                       "event_start_date_utc": "timestamp", 
                                       "event_end_date_utc": "timestamp", 
                                       "reporting_start_date_utc": "timestamp",
                                       "reporting_end_date_utc": "timestamp"})
    if updated:
        engine.execute("CREATE INDEX ON rpa.rpa_djpr_equities_" + str(year) + " (rp_entity_id, timestamp_utc)")
    return updated

updated = [ update_equities(year) for year in avail_years]

def select(cols):
    sql = 'SELECT "' + '", "'.join(cols) + '"'
    return sql

def get_sql(year, cols):
    return(select(cols) + "\nFROM rpa.rpa_djpr_equities_%s\n" % year)

if any(updated):

    metadata = MetaData(engine, schema="rpa")
    metadata.reflect(schema = "rpa")
    cols = [ column.name for column in metadata.tables['rpa.rpa_djpr_equities_' + str(max(avail_years))].columns]
    sql_parts = [get_sql(year, cols) for year in avail_years]

    sql = "CREATE VIEW rpa_djpr_equities AS\n" + 'UNION ALL\n'.join(sql_parts)

    conn = engine.connect()
    trans = conn.begin()
    conn.execute("DROP VIEW IF EXISTS rpa.rpa_djpr_equities CASCADE")
    conn.execute(sql)
    conn.execute("ALTER VIEW rpa.rpa_djpr_equities OWNER TO rpa")
    conn.execute("GRANT SELECT ON rpa.rpa_djpr_equities TO rpa_access")
    trans.commit()
    conn.close()
