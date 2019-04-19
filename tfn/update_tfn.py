#!/usr/bin/env python3
from sqlalchemy import create_engine, Boolean, MetaData, Table
from wrds2pg import wrds_update, run_file_sql
import sys
sys.path.insert(0, '..')

from make_engine import engine, wrds_id

def mod_col(column, schema, table, engine):
    command = "ALTER TABLE " + schema + "." + table + \
              " ALTER COLUMN " + column + " TYPE boolean USING (" + column + "=1)"
    engine.execute(command)
    return column

def is_col_to_bool(engine, schema, table):
    """
    This function changes type of columns named "is_" to boolean
    The table is from PostgreSQL, originally from wrds_id
    """
    the_table = Table(table, MetaData(), schema=schema, autoload=True,
                      autoload_with=engine)
    columns = the_table.c

    col_lst = [col.name for col in columns
                  if col.name.startswith("is_") and not isinstance(col.type, Boolean)]

    modify_lst = [mod_col(col, schema, table, engine) for col in col_lst]
    if modify_lst:
    	print("Columns changed to boolean", modify_lst)

    return modify_lst

updated = wrds_update("amend", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("avgreturns", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("company", "tfn", engine=engine, wrds_id=wrds_id, fix_cr=True)
updated = wrds_update("form144", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("header", "tfn", engine=engine, wrds_id=wrds_id)
# updated = wrds_update("idfhist", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("idfnames", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("rule10b5", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("table1", "tfn", engine=engine, wrds_id=wrds_id)
updated = wrds_update("table2", "tfn", engine=engine, wrds_id=wrds_id)


