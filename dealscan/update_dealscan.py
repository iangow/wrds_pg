#!/usr/bin/env python3
from wrds2pg import wrds_update, wrds_id
from sqlalchemy import create_engine

def get_wrds_tables(schema, wrds_id):

    from sqlalchemy import MetaData

    wrds_engine = create_engine("postgresql://%s@wrds-pgdata.wharton.upenn.edu:9737/wrds" % wrds_id,
                                connect_args = {'sslmode':'require'})

    metadata = MetaData(wrds_engine, schema=schema)
    metadata.reflect(schema=schema)

    table_list = [key.name for key in metadata.tables.values()]
    wrds_engine.dispose()
    return table_list

dealscan_tables = get_wrds_tables("tr_dealscan", wrds_id)

for table in dealscan_tables:
    wrds_update(table, "dealscan")
