#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine, process_sql

engine = make_engine()

updated = wrds_update("wrds_gvkey", "ciq", fix_missing=True)
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_gvkey (companyid)", engine=engine)

updated = wrds_update("wrds_cusip", "ciq", fix_missing=True)
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_cusip (companyid)", engine=engine)

updated = wrds_update("wrds_cik", "ciq", fix_missing=True)
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_cik (companyid)", engine=engine)

updated = wrds_update("ciqfininstance", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqfininstance (financialinstanceid)", engine=engine)
    process_sql("CREATE INDEX ON ciq.ciqfininstance (financialperiodid)", engine=engine)
    process_sql("ANALYZE ciq.ciqfininstance", engine=engine)
    
updated = wrds_update("ciqfinperiod", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqfinperiod (financialperiodid)", engine=engine)
    process_sql("CREATE INDEX ON ciq.ciqfinperiod (companyid)", engine=engine)
    process_sql("ANALYZE ciq.ciqfinperiod", engine=engine)
    
updated = wrds_update("ciqgvkeyiid", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqgvkeyiid (relatedcompanyid)", engine=engine)
    process_sql("ANALYZE ciq.ciqgvkeyiid", engine=engine)

updated = wrds_update("ciqfininstancetocollection", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqfininstancetocollection (financialcollectionid)", engine=engine)
    process_sql("CREATE INDEX ON ciq.ciqfininstancetocollection (financialinstanceid)", engine=engine)

engine.dispose()
