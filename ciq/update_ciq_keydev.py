#!/usr/bin/env python3
from sqlalchemy import create_engine

from wrds2pg.wrds2pg import wrds_update

updated = wrds2pg.wrds_update("wrds_keydev", "ciq", fix_missing=True)
if updated:
    engine = make_engine()
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid)")
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (companyid)")
    engine.dispose()
    
wrds_update("ciqkeydeveventtype", "ciq", fix_cr=True)
wrds_update("ciqkeydev", "ciq", fix_cr=True)
wrds_update("ciqkeydevstatus", "ciq")
wrds_update("ciqkeydevobjectroletype", "ciq", fix_cr=True)
wrds_update("ciqkeydevtoobjecttoeventtype", "ciq")
