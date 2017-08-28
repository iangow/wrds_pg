#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update, run_file_sql

updated = wrds_update("anncomp", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.anncomp (gvkey)")

updated = wrds_update("adsprate", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.adsprate (gvkey, datadate)")

updated = wrds_update("co_hgic", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.co_hgic (gvkey)")

updated = wrds_update("co_ifndq", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.co_ifndq (gvkey, datadate)")

company_updated = wrds_update("company", "comp", engine, wrds_id)
if company_updated:
    engine.execute("CREATE INDEX ON comp.company (gvkey)")

updated = wrds_update("idx_ann", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.idx_ann (datadate)")

updated = wrds_update("idx_index", "comp", engine, wrds_id)

updated = wrds_update("io_qbuysell", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.io_qbuysell (gvkey, datadate)")

updated = wrds_update("wrds_segmerged", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.wrds_segmerged (gvkey, datadate);")

updated = wrds_update("names", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.names (gvkey)")

secm_updated = wrds_update("secm", "comp", engine, wrds_id)
if secm_updated:
    engine.execute("CREATE INDEX ON comp.secm (gvkey, datadate)")

if secm_updated or company_updated:
    run_file_sql("comp/create_ciks.sql", engine)

updated = wrds_update("secd", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.secd (gvkey, datadate)")

updated = wrds_update("spind_mth", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.spind_mth (gvkey, datadate)")

updated = wrds_update("funda", "comp", engine, wrds_id,
                      fix_missing = True, rename="do=do_")
if updated:
    engine.execute("CREATE INDEX ON comp.funda (gvkey, datadate)")

updated = wrds_update("fundq", "comp", engine, wrds_id, fix_missing = True)
if updated:
    engine.execute("CREATE INDEX ON comp.fundq (gvkey, datadate)")

updated = wrds_update("g_sec_divid", "comp", engine, wrds_id,
                      fix_missing = True, rename="do=do_")
if updated:
    engine.execute("CREATE INDEX ON comp.g_sec_divid (gvkey, datadate)")

# updated = wrds_update("idxcst_his", "comp", engine, wrds_id, rename="from=fromdt")
