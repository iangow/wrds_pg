#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds2pg import wrds2pg
from time import gmtime, strftime

updated = wrds2pg.wrds_update("g_exrt_dly", "comp")

updated = wrds2pg.wrds_update("g_funda", "comp", fix_missing = True, rename="do=do_")
if updated:
    engine.execute("CREATE INDEX ON comp.g_funda (gvkey)")

updated = wrds2pg.wrds_update("g_secd", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.g_secd (gvkey)")

updated = wrds2pg.wrds_update("g_security", "comp")
updated = wrds2pg.wrds_update("g_company", "comp")

updated = wrds2pg.wrds_update("sec_history", "comp")
ecupdated = wrds2pg.wrds_update("idxcst_his", "comp", rename="from=fromdt")

updated = wrds2pg.wrds_update("anncomp", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.anncomp (gvkey)")

updated = wrds2pg.wrds_update("adsprate", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.adsprate (gvkey, datadate)")

updated = wrds2pg.wrds_update("co_hgic", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.co_hgic (gvkey)")

updated = wrds2pg.wrds_update("co_ifndq", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.co_ifndq (gvkey, datadate)")

company_updated = wrds2pg.wrds_update("company", "comp")
if company_updated:
    engine.execute("CREATE INDEX ON comp.company (gvkey)")

updated = wrds2pg.wrds_update("idx_ann", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.idx_ann (datadate)")

updated = wrds2pg.wrds_update("idx_index", "comp")

updated = wrds2pg.wrds_update("io_qbuysell", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.io_qbuysell (gvkey, datadate)")

updated = wrds2pg.wrds_update("wrds_segmerged", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.wrds_segmerged (gvkey, datadate);")

updated = wrds2pg.wrds_update("names", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.names (gvkey)")

secm_updated = wrds2pg.wrds_update("secm", "comp")
if secm_updated:
   engine.execute("CREATE INDEX ON comp.secm (gvkey, datadate)")

if secm_updated or company_updated:
    wrds2pg.run_file_sql("create_ciks.sql", engine)
    sql = "Created using update_comp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime())
    wrds2pg.set_table_comment("ciks", "comp", sql, engine)
    

updated = wrds2pg.wrds_update("secd", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.secd (gvkey, datadate)")

updated = wrds2pg.wrds_update("spind_mth", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.spind_mth (gvkey, datadate)")

updated = wrds2pg.wrds_update("funda", "comp", fix_missing = True, rename="do=do_")
if updated:
    engine.execute("CREATE INDEX ON comp.funda (gvkey, datadate)")

updated = wrds2pg.wrds_update("fundq", "comp", fix_missing = True)
if updated:
    engine.execute("CREATE INDEX ON comp.fundq (gvkey, datadate)")

updated = wrds2pg.wrds_update("g_sec_divid", "comp", fix_missing = True)
if updated:
    engine.execute("CREATE INDEX ON comp.g_sec_divid (gvkey, datadate)")

updated = wrds2pg.wrds_update("idxcst_his", "comp", rename="from=fromdt")
updated = wrds2pg.wrds_update("g_idxcst_his", "comp", rename="from=fromdt")
updated = wrds2pg.wrds_update("names_ix", "comp")
updated = wrds2pg.wrds_update("g_names_ix", "comp")
updated = wrds2pg.wrds_update("idxcst_his", "comp", rename="from=fromdt")

updated = wrds2pg.wrds_update("funda_fncd", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.funda_fncd (gvkey, datadate)")

updated = wrds2pg.wrds_update("fundq_fncd", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.fundq_fncd (gvkey, datadate)")

updated = wrds2pg.wrds_update("r_giccd", "comp")

updated = wrds2pg.wrds_update("r_datacode", "comp")
updated = wrds2pg.wrds_update("aco_pnfnda", "comp")
if updated:
    engine.execute("CREATE INDEX ON comp.aco_pnfnda (gvkey, datadate)")

updated = wrds2pg.wrds_update("names", "comp")
updated = wrds2pg.wrds_update("sec_divid", "comp", fix_missing = True)
if updated:
    engine.execute("CREATE INDEX ON comp.g_sec_divid (gvkey, datadate)")
