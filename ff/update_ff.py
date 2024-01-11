#!/usr/bin/env python3
from wrds2pg import wrds_update, get_wrds_tables

ff_tables = get_wrds_tables("ff_all")

for table in ff_tables:
    wrds_update(table, "ff")
