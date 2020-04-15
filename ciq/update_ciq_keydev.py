#!/usr/bin/env python3
from wrds2pg import make_engine, wrds_update

updated = wrds_update("wrds_keydev", "ciq", fix_missing=True)
if updated:
    engine = make_engine()
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid)")
    engine.execute("CREATE INDEX ON ciq.wrds_keydev (companyid)")
    engine.dispose()


updated = wrds_update("ciqkeydev", "ciq", fix_cr=True)
if updated:
    engine = make_engine()
    engine.execute("CREATE INDEX ON ciq.ciqkeydev (keydevid)")
    engine.dispose()

wrds_update("ciqkeydeveventtype", "ciq", fix_cr=True)
wrds_update("ciqkeydevstatus", "ciq")
wrds_update("ciqkeydevobjectroletype", "ciq", fix_cr=True)
updated = wrds_update("ciqkeydevtoobjecttoeventtype", "ciq")
if updated:
    engine = make_engine()
    engine.execute("CREATE INDEX ON ciq.ciqkeydevtoobjecttoeventtype (keydevid)")
    engine.dispose()

