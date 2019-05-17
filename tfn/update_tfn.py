#!/usr/bin/env python3
from sqlalchemy import create_engine, Boolean, MetaData, Table
import os, sys
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds2pg import wrds2pg
from time import gmtime, strftime


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

updated = wrds2pg.wrds_update("amend", "tfn")
updated = wrds2pg.wrds_update("avgreturns", "tfn")
updated = wrds2pg.wrds_update("company", "tfn", fix_cr=True)
updated = wrds2pg.wrds_update("form144", "tfn")
updated = wrds2pg.wrds_update("header", "tfn")
# updated = wrds2pg.wrds_update("idfhist", "tfn")
updated = wrds2pg.wrds_update("idfnames", "tfn")
updated = wrds2pg.wrds_update("rule10b5", "tfn")
updated = wrds2pg.wrds_update("table1", "tfn")
updated = wrds2pg.wrds_update("table2", "tfn")


