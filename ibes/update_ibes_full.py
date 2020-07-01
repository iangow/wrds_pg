#!/usr/bin/env python3
from wrds2pg import wrds_update, wrds_id
import wrds
db = wrds.Connection(wrds_username=wrds_id)
tables = db.list_tables(library='ibes')

for table in tables:
    fix_missing = table == "currnew"
    if table == "det_epsint":
        col_types = {'usfirm':'boolean',
                     'anntims':'text'}
    else:
        col_types = {'usfirm':'boolean'}
    wrds_update(table, "ibes",
                col_types = col_types,
                fix_missing = fix_missing)
