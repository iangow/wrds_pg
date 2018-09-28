#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
sys.path.insert(0, '..')
from wrds_fetch import wrds_update

dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
engine = create_engine("postgresql://" + host + "/" + dbname)

# Import sas file to pg
fpath = os.path.dirname(os.path.abspath(__file__))
print("fpath: %s" % fpath)
# Table_name should be the same as sas file, eg. for admit.sas7bdat, table_name = "admit"

table_name = 'msp500'
schema = 'audit'

#wrds_update(table_name, fpath, schema, engine, wrds_id="", fix_missing=True, fix_cr=True,
# drop="id name", obs="10", rename="fee=fee_old")

wrds_update(table_name=table_name, schema=schema, engine=engine, fpath=fpath)
