#!/usr/bin/env python3
from db2pq import wrds_update_pg, process_sql


updated = wrds_update_pg("wrds_gvkey", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_gvkey (companyid)")

updated = wrds_update_pg("wrds_cusip", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_cusip (companyid)")

updated = wrds_update_pg("wrds_cik", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.wrds_cik (companyid)")

updated = wrds_update_pg("ciqfininstance", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqfininstance (financialinstanceid)")
    process_sql("CREATE INDEX ON ciq.ciqfininstance (financialperiodid)")
    process_sql("ANALYZE ciq.ciqfininstance")
    
updated = wrds_update_pg("ciqfinperiod", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqfinperiod (financialperiodid)")
    process_sql("CREATE INDEX ON ciq.ciqfinperiod (companyid)")
    process_sql("ANALYZE ciq.ciqfinperiod")
    
updated = wrds_update_pg("ciqgvkeyiid", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqgvkeyiid (relatedcompanyid)")
    process_sql("ANALYZE ciq.ciqgvkeyiid")

updated = wrds_update_pg("ciqfininstancetocollection", "ciq")
if updated:
    process_sql("CREATE INDEX ON ciq.ciqfininstancetocollection (financialcollectionid)")
    process_sql("CREATE INDEX ON ciq.ciqfininstancetocollection (financialinstanceid)")
