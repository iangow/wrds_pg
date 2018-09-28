#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
from wrds_fetch_local import wrds_update_local

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

wrds_update_local(table_name, fpath, schema, engine, wrds_id="", fix_missing=False,
	fix_cr=False, drop="", obs="", rename="")
