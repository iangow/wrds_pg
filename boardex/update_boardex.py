#!/usr/bin/env python3
from wrds2pg import get_modified_str
from wrds2pg import wrds_update, make_engine

import re
from os import getenv
from sqlalchemy import create_engine
wrds_id = getenv("WRDS_ID")

def get_wrds_tables(schema, wrds_id):
    
    from sqlalchemy import MetaData
    
    wrds_engine = create_engine("postgresql://%s@wrds-pgdata.wharton.upenn.edu:9737/wrds" % wrds_id, 
                                connect_args = {'sslmode':'require'})

    metadata = MetaData(wrds_engine, schema=schema)
    metadata.reflect(schema=schema)

    table_list = [key.name for key in metadata.tables.values()]
    wrds_engine.dispose()
    return table_list

def update_schema(schema, wrds_id):

    table_list = get_wrds_tables(schema, wrds_id)
    
    regex_list = ["wrds_company_networks", "company_profile_sr_mgrs",
                  "wrds_dir_profile_emp", "dir_profile_emp",
                  "wrds_dir_profile_all", "wrds_org_composition"]
    regex = "(" + "|".join(regex_list) + ")"

    for table in table_list:
        if re.search("individual_networks", table):
            continue

        fix_missing = re.search(regex, table) is not None
        wrds_update(table_name=table, schema="boardex", 
                    wrds_id=wrds_id, fix_missing = fix_missing)

schemas = ["boardex_uk","boardex_row", "boardex_eur", "boardex_na"]
for schema in schemas:
    update_schema(schema, wrds_id)
