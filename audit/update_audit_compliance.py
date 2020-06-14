#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine
from sqlalchemy import Table, MetaData, Boolean

engine = make_engine()

def mod_col_bool(column, schema, table, engine):
    command = "ALTER TABLE " + schema + "." + table + \
              " ALTER COLUMN " + column + " TYPE boolean USING (" + column + "=1)"
    engine.execute(command)
    return column
    
def mod_col_date(column, schema, table, engine):
    command = "ALTER TABLE " + schema + "." + table + \
              " ALTER COLUMN " + column + " TYPE integer USING (" + column + "::date)"
    engine.execute(command)
    return column

def mod_col_int(column, schema, table, engine):
    command = "ALTER TABLE " + schema + "." + table + \
              " ALTER COLUMN " + column + " TYPE integer USING (" + column + "::integer)"
    engine.execute(command)
    return column

def is_col_to_bool(engine, schema, table):
    """
    This function changes type of columns named "is_" to boolean
    The table is from PostgreSQL, originally from wrds_id
    """
    the_table = Table(table, MetaData(), schema=schema, autoload=True,
                      autoload_with=engine)
    columns = the_table.c

    col_lst = [col.name for col in columns
                  if col.name.startswith("is_") and not isinstance(col.type, Boolean)]

    modify_lst = [mod_col_bool(col, schema, table, engine) for col in col_lst]
    if modify_lst:
    	print("Columns changed to boolean:", modify_lst)

    return modify_lst
    
def iss_col_to_bool(engine, schema, table):
    """
    This function changes type of columns named "iss_" to boolean
    The table is from PostgreSQL, originally from wrds_id
    """
    the_table = Table(table, MetaData(), schema=schema, autoload=True,
                      autoload_with=engine)
    columns = the_table.c

    col_lst = [col.name for col in columns
                  if (col.name.startswith("iss_") or col.name.startswith("aud_")) 
                        and not isinstance(col.type, Boolean)]

    modify_lst = [mod_col_bool(col, schema, table, engine) for col in col_lst
                        if col != "iss_other_text"]
    if modify_lst:
    	print("Columns changed to boolean:", modify_lst)

    return modify_lst

def col_to_int(engine, schema, table, col_lst=None):   
    """
    This function changes provided columns to integer.
    """

    if col_lst:
        modify_lst = [mod_col_int(col, schema, table, engine) for col in col_lst]
    else:
        print("No columns supplied!")
        return None
        
    if modify_lst:
    	print("Columns changed to integer:", modify_lst)

    return modify_lst

def col_to_bool(engine, schema, table, col_lst=None):
    """
    This function changes provided columns to boolean.
    """

    if col_lst:
        modify_lst = [mod_col_bool(col, schema, table, engine) for col in col_lst]
    else:
        print("No columns supplied!")
        return None
        
    if modify_lst:
    	print("Columns changed to boolean:", modify_lst)

    return modify_lst

# Auditors
updated = wrds_update("auditorsinfo", "audit", 
                      col_types = {"auditor_key": "integer",
                                   "pcaob_reg_num": "integer",
                                   "pcaob_app_num": "integer",
                                  "aud_loc_key": "integer", 
                                   "eventdate_aud_fkey": "integer", 
                                   "auditor_pcaob_reg": "boolean"})

# Auditor Changes
updated = wrds_update("auditchange", "audit", 
                      col_types = {"auditor_change_key": "integer",
                                   "dismiss_key": "integer",
                                   "engaged_auditor_key": "integer",
                                  "dismissed_gc": "boolean", 
                                   "dismissed_disagree": "boolean", 
                                   "auditor_resigned": "boolean",
                                   "dismiss_pcaob_reg": "boolean",
                                   "merger": "boolean",
                                   "is_benefit_plan": "boolean",
                                   "engaged_auditor_pcaob": "boolean"}, 
                      drop="match: prior: closest: dismiss_name " + 
                            "engaged_auditor_name eventdate_aud_name")
if updated:
    iss_col_to_bool(engine, "audit", "auditchange")

# Audit Fees
updated = wrds_update("auditfees", "audit", 
                      drop="match: prior: closest: auditor_name eventdate_aud_name", 
                      col_types = {"eventdate_aud_fkey":"integer", 
                                   "auditor_fkey":"integer", 
                                   "audit_gig_key":"integer",
                                   "fiscal_year":"integer",
                                   "restatement":"boolean",
                                   "fees_pcaob_reg":"boolean",
                                   "is_benefit_plan":"boolean"})
               
# Audit Fees with Restatements
updated = wrds_update("auditfeesr", "audit", 
                      drop="match: prior: closest: auditor_name eventdate_aud_name", 
                      col_types = {"eventdate_aud_fkey":"integer", 
                                   "auditor_fkey":"integer", 
                                   "audit_gig_key":"integer",
                                   "fiscal_year":"integer",
                                   "restatement":"boolean",
                                   "fees_pcaob_reg":"boolean",
                                   "is_benefit_plan":"boolean"})

# Audit Opinions
updated = wrds_update("auditopin", "audit",
                      drop="match: prior: closest:", 
                      col_types = {"audit_op_key": "integer", 
                                   "auditor_fkey": "integer",
                                   "auditor_affil_fkey": "integer",
                                   "going_concern": "boolean",
                                   "op_aud_pcaob": "boolean",
                                   "eventdate_aud_fkey": "integer",
                                   "fiscal_year_of_op": "integer"})

updated = wrds_update("revauditopin", "audit",
                      drop="match: prior: closest:", 
                      col_types = {"audit_op_key": "integer", 
                                   "auditor_fkey": "integer",
                                   "auditor_affil_fkey": "integer",
                                   "is_nth_add_op": "integer",
                                   "going_concern": "boolean",
                                   "op_aud_pcaob": "boolean",
                                   "eventdate_aud_fkey": "integer",
                                   "fiscal_year_of_op": "integer"})

# opinion_text1 <chr>, opinion_text2 <chr>, opinion_text3?
updated = wrds_update("benefit", "audit", 
                      drop="match: prior: closest:",
                      col_types = {"benefit_plan_key": "integer", 
                                   "auditor_fkey": "integer",
                                   "auditor_affil_fkey": "integer",
                                   "is_nth_opinion": "integer",
                                   "op_aud_pcaob": "boolean",
                                   "going_concern": "boolean",
                                   "eventdate_aud_fkey": "integer",
                                   "fiscal_year_of_op": "integer"})

updated = wrds_update("auditors", "audit", 
                       col_types = {"auditor_key": "integer"}) 

# Non-reliance restatements
updated = wrds_update("auditnonreli", "audit", 
                        drop="prior: match: closest: disc_text:",
                        col_types = {"res_accounting": "boolean",
                                    "res_fraud": "boolean", 
                                    "res_cler_err": "boolean",
                                    "res_adverse": "boolean", 
                                    "res_improves": "boolean", 
                                    "res_other": "boolean",
                                    "res_sec_invest": "boolean",
                                    "res_begin_aud_fkey": "integer", 
                                    "res_notif_key": "integer", 
                                    "current_aud_fkey": "integer", 
                                    "res_begin_aud_fkey": "integer", 
                                    "res_end_aud_fkey": "integer", 
                                    "file_date_aud_fkey": "integer"})

if updated:
    list_cols = ["res_acc_res_fkey_list", "res_fraud_res_fkey_list",
                "res_cler_err_fkey_list", "res_period_aud_fkey", "res_other_rescat_fkey"]
    for col in list_cols:
        engine.execute("""
            ALTER TABLE audit.auditnonreli
            ALTER COLUMN %s TYPE integer[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::integer[] """ % (col, col))
                    
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON audit.auditnonreli (res_notif_key)")
    
# SOX 302 Disclosure Controls   
updated = wrds_update("auditsox302", "audit",
                      drop="prior: match: closest: ic_dc_text:", 
                      col_types = {"ic_dc_key": "integer", 
                                    "is_effective": "integer",
                                    "material_weakness": "boolean",
                                    "sig_deficiency": "boolean",
                                    "noteff_acc_rule": "integer",
                                    "noteff_fin_fraud": "integer",
                                    "notefferrors": "integer",
                                    "noteff_other": "integer",
                                    "eventdate_aud_fkey": "integer"})

if updated:
    list_cols = ["noteff_acc_reas_keys", "noteff_finfraud_keys", 
                    "noteff_reas_keys", "noteff_other_reas_keys"]
    for col in list_cols:
        engine.execute("""
            ALTER TABLE audit.auditsox302
            ALTER COLUMN %s TYPE integer[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::integer[] """ % (col, col))
                    
    list_cols = ["noteff_acc_reas_phr", "noteff_finfraud_phr", 
                    "noteff_reas_phr", "noteff_other_reas_phr"]
    for col in list_cols:
        engine.execute("""
            ALTER TABLE audit.auditsox302
            ALTER COLUMN %s TYPE text[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::text[] """ % (col, col))
    
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON audit.auditsox302 (ic_dc_key)")


updated = wrds_update("auditsox302", "audit", 
                      keep="ic_dc_key ic_dc_text:",
                      alt_table_name="auditsox302_text",
                      col_types = {"ic_dc_key": "integer"})

# SOX 404 Internal Controls
updated = wrds_update("auditsox404", "audit",
                      drop="prior: match: closest: ic_text:",
                        col_types = {"ic_op_fkey": "integer",
                                     "auditor_fkey": "integer", 
                                     "eventdate_aud_fkey": "integer"})

updated = wrds_update("auditsox404", "audit", 
                      keep="ic_op_fkey ic_text1",
                      alt_table_name="auditsox404_text1",
                      col_types = {"ic_op_fkey": "integer"})
updated = wrds_update("auditsox404", "audit", 
                      keep="ic_op_fkey ic_text2",
                      alt_table_name="auditsox404_text2",
                      col_types = {"ic_op_fkey": "integer"})
# Accelerated Filer
updated = wrds_update("accfiler", "audit",
                      drop="prior: match: closest:",
                      col_types = {"accel_filer_key": "integer",
                                   "hst_season_issuer": "integer",   
                                   "hst_is_shell_co": "integer",                      
                                   "hst_is_accel_filer": "integer",     
                                   "hst_is_large_accel": "integer", 
                                   "hst_is_voluntary_filer": "integer", 
                                   "hst_is_small_report": "integer",   
                                   "did_not_disc": 'boolean',
                                   "eventdate_aud_fkey": "integer"})                     

# Director and officer changes
updated = wrds_update("diroffichange", "audit", 
                        drop="match: prior: closest: do_change_text:",
                        col_types = {"do_off_pers_key": "integer",
                                     "interim": 'boolean',
                                     "do_off_remains": 'boolean',
                                     "retain_other_pos": 'boolean',
                                     "eff_date_unspec": 'boolean', 
                                     "eff_date_next_meet": 'boolean',
                                     'is_c_level': 'boolean', 
                                     'is_bdmem_pers': 'boolean', 
                                     'is_legal': 'boolean', 
                                     'is_scitech_pers': 'boolean', 
                                     'is_admin_pers': 'boolean', 
                                     'is_fin_pers': 'boolean', 
                                     'is_op_pers': 'boolean', 
                                     'is_cont': 'boolean', 
                                     'is_chair': 'boolean', 
                                     'is_chair_other': 'boolean', 
                                     'is_secretary': 'boolean', 
                                     'is_coo': 'boolean', 
                                     'is_president': 'boolean', 
                                     'is_ceo': 'boolean', 
                                     'is_cfo': 'boolean', 
                                     'is_exec_vp': 'boolean'})

if updated:                     
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON audit.diroffichange (do_off_pers_key)")
    
updated = wrds_update("diroffichange", "audit", 
                      keep="ftp_file_fkey do_change_text1",
                      alt_table_name="diroffichange_text1")

updated = wrds_update("diroffichange", "audit", 
                      keep="ftp_file_fkey do_change_text2",
                      alt_table_name="diroffichange_text2")

updated = wrds_update("diroffichange", "audit", 
                      keep="ftp_file_fkey do_change_text3",
                      alt_table_name="diroffichange_text3")

# Non-timely Filer Information And Analysis
# Partially working; need to add part4_3_text* columns
updated = wrds_update("nt", "audit", 
                      drop="match: closest: prior: part4_3_text:",
                      col_types = {"nt_notify_key": "integer"})
updated = wrds_update("nt", "audit", keep="nt_notify_key part4_3_text1",
                      alt_table_name = "nt_text1",
                      col_types = {"nt_notify_key": "integer"})
updated = wrds_update("nt", "audit", keep="nt_notify_key part4_3_text2",
                      alt_table_name = "nt_text2",
                      col_types = {"nt_notify_key": "integer"})                      
updated = wrds_update("nt", "audit", keep="nt_notify_key part4_3_text3",
                      alt_table_name = "nt_text3",
                      col_types = {"nt_notify_key": "integer"})
engine.dispose()
