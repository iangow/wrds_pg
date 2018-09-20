#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

# Step 1: read sas file and create table
fpath = '/home/jingyuz'
table_name = 'admit'
schema = 'public'
from wrds_fetch import *
make_table_data=get_table_sql(table_name, fpath, schema)
print(make_table_data)


# Step 2: get table create sql
# Now wrds_to_pg line 284
# get_wrds_process needs further fix
res = engine.execute("DROP TABLE IF EXISTS " + schema + "." + table_name + " CASCADE")
res = engine.execute(make_table_data["sql"])
p=get_wrds_process(table_name, fpath, schema, wrds_id)
wrds_process_to_pg(table_name, schema, engine, p)
