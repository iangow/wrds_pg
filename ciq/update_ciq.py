#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine

engine = make_engine()

updated = wrds_update("wrds_gvkey", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_gvkey (companyid)")

updated = wrds_update("wrds_cusip", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cusip (companyid)")

updated = wrds_update("wrds_cik", "ciq", fix_missing=True)
if updated:
    engine.execute("CREATE INDEX ON ciq.wrds_cik (companyid)")

updated = wrds_update("ciqfininstance", "ciq")
if updated:
    engine.execute("CREATE INDEX ON ciq.ciqfininstance (financialperiodid)")
    engine.execute("ANALYZE ciq.ciqfininstance")
    
updated = wrds_update("ciqfinperiod", "ciq")
if updated:
    engine.execute("CREATE INDEX ON ciq.ciqfinperiod (financialperiodid)")
    engine.execute("CREATE INDEX ON ciq.ciqfinperiod (companyid)")
    engine.execute("ANALYZE ciq.ciqfinperiod")
    
updated = wrds_update("ciqgvkeyiid", "ciq")
if updated:
    engine.execute("CREATE INDEX ON ciq.ciqgvkeyiid (relatedcompanyid)")
    engine.execute("ANALYZE ciq.ciqgvkeyiid")

engine.dispose()
