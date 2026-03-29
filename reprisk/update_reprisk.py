#!/usr/bin/env python3
from db2pq import wrds_update_pg, process_sql

updated = wrds_update_pg("v2_company_identifiers", "reprisk", force=True)
if updated:
    process_sql("CREATE INDEX ON reprisk.v2_company_identifiers (primary_isin)",)
    process_sql("CREATE INDEX ON reprisk.v2_company_identifiers (reprisk_id)",)

updated = wrds_update_pg("v2_risk_incidents", "reprisk")
if updated:
    process_sql("CREATE INDEX ON reprisk.v2_risk_incidents (reprisk_id)",)

updated = wrds_update_pg("v2_metrics", "reprisk")
if updated:
    process_sql("CREATE INDEX ON reprisk.v2_metrics (reprisk_id)")

updated = wrds_update_pg("v2_wrds_company_id_table", "reprisk")
if updated:
    process_sql("CREATE INDEX ON reprisk.v2_wrds_company_id_table (reprisk_id)")
