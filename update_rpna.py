#!/usr/bin/env python3
from sqlalchemy import create_engine, MetaData
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update

avail_years = range(2000, 2019)

def update_equities(year):
    updated = wrds_update("pr_equities_" + str(year), "rpna", engine, wrds_id, rename="group=group_")
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
