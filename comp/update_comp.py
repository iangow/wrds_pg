#!/usr/bin/env python3
from time import gmtime, strftime
from wrds2pg import wrds_update, make_engine, run_file_sql, process_sql
engine = make_engine()

updated = wrds_update("g_exrt_dly", "comp")
updated = wrds_update("co_filedate", "comp")

updated = wrds_update("g_exrt_dly", "comp")

updated = wrds_update("g_funda", "comp", fix_missing = True)
if updated:
    process_sql("CREATE INDEX ON comp.g_funda (gvkey)", engine=engine)

updated = wrds_update("g_security", "comp")
updated = wrds_update("g_company", "comp")

updated = wrds_update("sec_history", "comp")
updated = wrds_update("idxcst_his", "comp")

updated = wrds_update("anncomp", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.anncomp (gvkey)", engine=engine)

updated = wrds_update("adsprate", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.adsprate (gvkey, datadate)", engine=engine)
    
updated = wrds_update("co_afnd2", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.co_afnd2 (gvkey)", engine=engine)

updated = wrds_update("co_hgic", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.co_hgic (gvkey)", engine=engine)

updated = wrds_update("co_ifndq", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.co_ifndq (gvkey, datadate)", engine=engine)

company_updated = wrds_update("company", "comp")
if company_updated:
    process_sql("CREATE INDEX ON comp.company (gvkey)", engine=engine)

updated = wrds_update("idx_ann", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.idx_ann (datadate)", engine=engine)

updated = wrds_update("idx_index", "comp")

updated = wrds_update("io_qbuysell", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.io_qbuysell (gvkey, datadate)", engine=engine)

# Update segment data
updated = wrds_update("seg_annfund", "comp")
updated = wrds_update("seg_customer", "comp")
updated = wrds_update("wrds_seg_customer", "comp")
updated = wrds_update("wrds_segmerged", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.wrds_segmerged (gvkey, datadate);", engine=engine)

updated = wrds_update("names", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.names (gvkey)", engine=engine)

secm_updated = wrds_update("secm", "comp")
if secm_updated:
   process_sql("CREATE INDEX ON comp.secm (gvkey, datadate)", engine=engine)

if secm_updated or company_updated:
    run_file_sql("comp/create_ciks.sql", engine)
    sql = "COMMENT ON TABLE comp.ciks IS 'Created using update_comp.py ON " + \
        strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

updated = wrds_update("spind_mth", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.spind_mth (gvkey, datadate)", engine=engine)

updated = wrds_update("funda", "comp", fix_missing = True)
if updated:
    process_sql("CREATE INDEX ON comp.funda (gvkey, datadate)", engine=engine)

updated = wrds_update("fundq", "comp", fix_missing = True)
if updated:
    process_sql("CREATE INDEX ON comp.fundq (gvkey, datadate)", engine=engine)

updated = wrds_update("g_sec_divid", "comp", fix_missing = True)
if updated:
    process_sql("CREATE INDEX ON comp.g_sec_divid (gvkey, datadate)", engine=engine)

updated = wrds_update("idxcst_his", "comp")
updated = wrds_update("g_idxcst_his", "comp")
updated = wrds_update("names_ix", "comp")
updated = wrds_update("g_names_ix", "comp")
updated = wrds_update("idxcst_his", "comp")

updated = wrds_update("funda_fncd", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.funda_fncd (gvkey, datadate)", engine=engine)

updated = wrds_update("fundq_fncd", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.fundq_fncd (gvkey, datadate)", engine=engine)

updated = wrds_update("r_giccd", "comp")
updated = wrds_update("r_auditors", "comp")

updated = wrds_update("r_datacode", "comp")
updated = wrds_update("aco_pnfnda", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.aco_pnfnda (gvkey, datadate)", engine=engine)

updated = wrds_update("names", "comp")
updated = wrds_update("sec_divid", "comp", fix_missing = True)
if updated:
    process_sql("CREATE INDEX ON comp.g_sec_divid (gvkey, datadate)", engine=engine)

updated = wrds_update("g_idxcst_his", "comp")
updated = wrds_update("g_idx_index", "comp")
updated = wrds_update("g_idx_mth", "comp")
updated = wrds_update("g_secnamesd", "comp")
updated = wrds_update("g_names_ix", "comp")
updated = wrds_update("g_names_ix_cst", "comp")
updated = wrds_update("g_names", "comp")
updated = wrds_update("g_namesq", "comp")
updated = wrds_update("g_chars", "comp")

# Footnotes
wrds_update("funda_fncd", "comp")
wrds_update("r_fndfntcd", "comp")
wrds_update("co_adesind", "comp")

updated = wrds_update("g_secd", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.g_secd (gvkey)", engine)

updated = wrds_update("g_secm", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.g_secm (gvkey)", engine)

updated = wrds_update("secd", "comp")
if updated:
    process_sql("CREATE INDEX ON comp.secd (gvkey, datadate)", engine)

engine.dispose()
