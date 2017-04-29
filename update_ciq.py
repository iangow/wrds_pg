#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
dbname = engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update

from wrds_fetch import wrds_update
updated = wrds_update("wrds_gvkey", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_gvkey (companyid)")

updated= wrds_update("wrds_cusip", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cusip (companyid)")
                   
updated = wrds_update("wrds_cik", "ciq", engine, wrds_id, fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cik (companyid)")
                   
updated = wrds_update("wrds_keydev", "ciq", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid)")
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (companyid)")

wrds_update("wrds_professional", "ciq", engine, wrds_id, fix_cr=True)
wrds_update("ciqkeydevobjectroletype", "ciq", engine, wrds_id)
wrds_update("ciqkeydeveventtype", "ciq", engine, wrds_id)