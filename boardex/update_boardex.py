#!/usr/bin/env python3
from db2pq import wrds_update_pg, wrds_get_tables

import re
    
def update_schema(schema):

    table_list = wrds_get_tables(schema, views=True)

    for table in table_list:
        # These tables are way too big and are redundant
        if re.search("individual_networks", table):
            continue

        # Table doesn't work and seems not to be maintained.
        if table == "wrds_company_dir_names":
            continue

        # BoardEx tables on WRDS PostgreSQL don't have update comments.
        # So we need to use SAS "Last updated" data.
        wrds_update_pg(table_name=table, schema=schema, use_sas=True)

update_schema("boardex")
