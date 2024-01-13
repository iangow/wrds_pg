#!/usr/bin/env python3
from wrds2pg import wrds_update, wrds_id, get_wrds_tables
from sqlalchemy import create_engine

tr_esg_tables = get_wrds_tables("tr_esg", wrds_id)

for table in tr_esg_tables:
    wrds_update(table, "tresg",
                col_types = {"orgpermid": "bigint"})
