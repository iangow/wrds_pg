#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine

engine = make_engine()

updated = wrds_update("v2_company_identifiers", "reprisk")
if updated:
    engine.execute("CREATE INDEX ON reprisk.v2_company_identifiers (primary_isin)")
    engine.execute("CREATE INDEX ON reprisk.v2_company_identifiers (reprisk_id)")

updated = wrds_update("v2_risk_incidents", "reprisk")
if updated:
    engine.execute("CREATE INDEX ON reprisk.v2_risk_incidents (reprisk_id)")

updated = wrds_update("v2_metrics", "reprisk")
if updated:
    engine.execute("CREATE INDEX ON reprisk.v2_metrics (reprisk_id)")

updated = wrds_update("v2_wrds_company_id_table", "reprisk")
if updated:
    engine.execute("CREATE INDEX ON reprisk.v2_wrds_company_id_table (reprisk_id)")
