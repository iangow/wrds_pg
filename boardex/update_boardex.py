#!/usr/bin/env python3
from wrds2pg import wrds_update, get_wrds_tables

import re
    
def update_schema(schema):

    table_list = get_wrds_tables(schema, views=True)

    regex_list = ["wrds_company_networks", "company_profile_sr_mgrs",
                  "wrds_dir_profile_emp", "dir_profile_emp",
                  "wrds_dir_profile_all", "wrds_org_composition"]
    regex = "(" + "|".join(regex_list) + ")"

    for table in table_list:
        # These tables are way too big and are redundant
        if re.search("individual_networks", table):
            continue

        # Table doesn't work and seems not to be maintained.
        if table == "wrds_company_dir_names":
            continue

        fix_missing = re.search(regex, table) is not None
        wrds_update(table_name=table, schema=schema,
                    fix_missing=fix_missing)

update_schema("boardex")
