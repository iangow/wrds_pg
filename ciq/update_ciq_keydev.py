#!/usr/bin/env python3
from db2pq import wrds_update_pg, process_sql

updated = wrds_update_pg("wrds_keydev", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid)")
    process_sql("CREATE INDEX ON ciq.wrds_keydev (companyid)")

updated = wrds_update_pg("ciqkeydev", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqkeydev (keydevid)")

wrds_update_pg("ciqkeydeveventtype", "ciq")
wrds_update_pg("ciqkeydevstatus", "ciq")
wrds_update_pg("ciqkeydevobjectroletype", "ciq")
updated = wrds_update_pg("ciqkeydevtoobjecttoeventtype", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqkeydevtoobjecttoeventtype (keydevid)")
