#!/usr/bin/env python3
from db2pg import wrds_update_pg, wrds_get_tables

ff_tables = wrds_get_tables("ff_all")

for table in ff_tables:
    wrds_update_pg(table, "ff")
