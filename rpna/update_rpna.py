#!/usr/bin/env python3
from sqlalchemy import MetaData
from wrds2pg import wrds_update, run_file_sql, make_engine
import datetime

engine = make_engine()

now = datetime.datetime.now()
avail_years = range(2000, now.year+1)

updated = wrds_update("rp_entity_mapping", "rpna")

def update_equities(year):
    updated = wrds_update("pr_equities_" + str(year), "rpna")
    if updated:
        engine.execute("CREATE INDEX ON rpna.pr_equities_" + str(year) + " (rp_entity_id, rpna_date_utc)")
    return updated

updated = [ update_equities(year) for year in avail_years]

def select(cols):
    sql = "SELECT " + ", ".join(cols)
    return sql

def get_sql(year, cols):
    return(select(cols) + "\nFROM rpna.pr_equities_%s\n" % year)

if any(updated):

    metadata = MetaData(engine, schema="rpna")
    metadata.reflect(schema = "rpna")
    cols = [ column.name for column in metadata.tables['rpna.pr_equities_' + str(max(avail_years))].columns]
    sql_parts = [get_sql(year, cols) for year in avail_years]

    sql = "CREATE VIEW rpna.pr_equities AS\n" + 'UNION ALL\n'.join(sql_parts)

    conn = engine.connect()
    trans = conn.begin()
    conn.execute("DROP VIEW IF EXISTS rpna.pr_equities CASCADE")
    conn.execute(sql)
    conn.execute("ALTER VIEW rpna.pr_equities OWNER TO rpna")
    conn.execute("GRANT SELECT ON rpna.pr_equities TO rpna_access")
    trans.commit()
    conn.close()
