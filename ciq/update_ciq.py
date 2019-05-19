#!/usr/bin/env python3
from sqlalchemy import create_engine
from wrds2pg import wrds_update, run_file_sql, make_engine
from wrds2pg import make_engine, wrds_id

engine = make_engine()

updated = wrds2pg.wrds_update("wrds_gvkey", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_gvkey (companyid)")

updated = wrds2pg.wrds_update("wciklink_gvkey", "wrdssec")
updated = wrds2pg.wrds_update("wciklink_names", "wrdssec")
# updated = wrds2pg.wrds_update("wciklink_cusip", "wrdssec")

updated = wrds2pg.wrds_update("wrds_cusip", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cusip (companyid)")

updated = wrds2pg.wrds_update("wrds_cik", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cik (companyid)")

updated = wrds2pg.wrds_update("wrds_keydev", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid)")
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (companyid)")

wrds2pg.wrds_update("wrds_professional", "ciq", fix_cr=True)
wrds2pg.wrds_update("ciqkeydeveventtype", "ciq", fix_cr=True)
wrds2pg.wrds_update("ciqkeydevobjectroletype", "ciq", fix_cr=True)
wrds2pg.wrds_update("ciqfininstance", "ciq")
wrds2pg.wrds_update("ciqfinperiod", "ciq")
wrds2pg.wrds_update("ciqgvkeyiid", "ciq")
wrds2pg.wrds_update("ciqkeydevstatus", "ciq")
wrds2pg.wrds_update("ciqkeydev", "ciq", fix_cr=True)
engine.dispose()
