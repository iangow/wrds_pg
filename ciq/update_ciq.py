#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
dbname = engine = create_engine("postgresql://" + host + "/" + dbname)

sys.path.insert(0, '..')
from wrds_fetch import wrds_update

updated = wrds_update("wrds_gvkey", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_gvkey (companyid)")
updated = wrds_update("wciklink_gvkey", "wrdssec", engine, wrds_id)
updated = wrds_update("wciklink_names", "wrdssec", engine, wrds_id)
updated = wrds_update("wciklink_cusip", "wrdssec", engine, wrds_id)

updated = wrds_update("wrds_cusip", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cusip (companyid)")

updated = wrds_update("wrds_cik", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cik (companyid)")

updated = wrds_update("wrds_keydev", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid)")
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (companyid)")

wrds_update("wrds_professional", "ciq", engine, wrds_id, fix_cr=True)
wrds_update("ciqkeydeveventtype", "ciq", engine, wrds_id, fix_cr=True)
wrds_update("ciqkeydevobjectroletype", "ciq", engine, wrds_id, fix_cr=True)
wrds_update("ciqfininstance", "ciq", engine, wrds_id)
wrds_update("ciqfinperiod", "ciq", engine, wrds_id)
wrds_update("ciqgvkeyiid", "ciq", engine, wrds_id)
wrds_update("ciqkeydevstatus", "ciq", engine, wrds_id)
wrds_update("ciqkeydev", "ciq", engine, wrds_id, fix_cr=True)
