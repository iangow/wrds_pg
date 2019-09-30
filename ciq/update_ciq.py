#!/usr/bin/env python3
from wrds2pg.wrds2pg import wrds_update, make_engine

engine = make_engine()

wrds_update("ciqgvkeyiid", "ciq")
updated = wrds_update("wrds_gvkey", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_gvkey (companyid)")

updated = wrds_update("wrds_cusip", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cusip (companyid)")

updated = wrds_update("wrds_cik", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cik (companyid)")

wrds_update("ciqfininstance", "ciq")
wrds_update("ciqfinperiod", "ciq")

engine.dispose()
