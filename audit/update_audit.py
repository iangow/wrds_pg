#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine
from sqlalchemy import Table, MetaData, Boolean

engine = make_engine()

def mod_col(column, schema, table, engine):
    command = "ALTER TABLE " + schema + "." + table + \
              " ALTER COLUMN " + column + " TYPE boolean USING (" + column + "=1)"
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

    modify_lst = [mod_col(col, schema, table, engine) for col in col_lst]
    if modify_lst:
    	print("Columns changed to boolean:", modify_lst)

    return modify_lst
    
def col_to_bool(engine, schema, table, col_lst=None):
    """
    This function changes provided columns to boolean.
    """

    if col_lst:
        modify_lst = [mod_col(col, schema, table, engine) for col in col_lst]
    else:
        print("No columns supplied!")
        return None
        
    if modify_lst:
    	print("Columns changed to boolean:", modify_lst)

    return modify_lst

# Transfer agents
updated = wrds_update("feed41_transfer_agents", "audit")

# Non-reliance restatements
updated = wrds_update("auditnonreli", "audit",
                        drop="prior: match: closest: disc_text:")

if updated:
    col_to_bool(engine, "audit", "auditnonreli", 
                col_lst=["res_accounting", "res_fraud", "res_cler_err",
                         "res_adverse", "res_improves", "res_other",
                         "res_sec_invest"])
    int_cols = ["res_begin_aud_fkey", "res_notif_key", "current_aud_fkey", 
                "res_begin_aud_fkey", "res_end_aud_fkey", "file_date_aud_fkey"]
    for col in int_cols:
        engine.execute("""
            ALTER TABLE audit.auditnonreli
            ALTER COLUMN %s TYPE integer USING %s::integer""" % (col, col))
    
    list_cols = ["res_acc_res_fkey_list", "res_fraud_res_fkey_list",
                "res_cler_err_fkey_list", "res_period_aud_fkey", "res_other_rescat_fkey"]
    for col in list_cols:
        engine.execute("""
            ALTER TABLE audit.auditnonreli
            ALTER COLUMN %s TYPE integer[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::integer[] """ % (col, col))
                    
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON audit.auditnonreli (res_notif_key)")

updated = wrds_update("feed09tocat", "audit")
if updated:
    engine.execute("ALTER TABLE audit.feed09tocat ALTER res_notify_key TYPE integer")
    engine.execute("ALTER TABLE audit.feed09tocat ALTER res_category_fkey TYPE integer")

updated = wrds_update("feed09period", "audit")
if updated:
    engine.execute("ALTER TABLE audit.feed09period ALTER res_notify_key TYPE integer")
    engine.execute("ALTER TABLE audit.feed09period ALTER res_period_aud_fkey TYPE integer USING res_period_aud_fkey::double precision")

updated = wrds_update("feed09filing", "audit", drop="file_date_num", force=True)
if updated:
    engine.execute("""
        ALTER TABLE audit.feed09filing
        RENAME COLUMN res_notify_key TO res_notif_key""")
    
    int_cols = ["res_filing_key", "res_notif_key"]
    for col in int_cols:
        engine.execute("""
            ALTER TABLE audit.feed09filing
            ALTER COLUMN %s TYPE integer USING %s::integer""" % (col, col))
    engine.execute("CREATE INDEX ON audit.feed09filing (res_notif_key)")  
    engine.execute("CREATE INDEX ON audit.feed09filing (res_filing_key)")  

updated = wrds_update("feed09cat", "audit")

updated = wrds_update("feed25person", "audit")
updated = wrds_update("namesauditorsinfo", "audit")

# Partially working; need to add part4_3_text* columns
updated = wrds_update("nt", "audit", drop="match: closest: prior: part4_3_text:")
updated = wrds_update("nt", "audit", keep="nt_notify_key part4_3_text1",
                      alt_table_name = "nt_part4_3_text")

updated = wrds_update("bankrupt", "audit", drop="match: closest: prior:")
if updated:
    engine.execute("""
        ALTER TABLE audit.bankrupt ALTER COLUMN bank_key TYPE integer;
        ALTER TABLE audit.bankrupt ALTER COLUMN bankruptcy_type TYPE integer;
        ALTER TABLE audit.bankrupt ALTER COLUMN law_court_fkey TYPE integer;
        ALTER TABLE audit.bankrupt
            ALTER COLUMN court_type_code TYPE integer USING court_type_code::integer;
        ALTER TABLE audit.bankrupt ALTER COLUMN eventdate_aud_fkey TYPE integer;""")

# Director and officer changes
updated = wrds_update("diroffichange", "audit", 
                        drop="match: prior: closest: do_change_text:")
if updated:
    engine.execute("""
        ALTER TABLE audit.diroffichange
        ALTER COLUMN do_off_pers_key TYPE integer""")
    is_col_to_bool(engine, "audit", "diroffichange")
    col_to_bool(engine, "audit", "diroffichange", 
                col_lst=["interim", "do_off_remains", "retain_other_pos",
                         "eff_date_unspec", "eff_date_next_meet"])
                         
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON audit.diroffichange (do_off_pers_key)")

updated = wrds_update("feed17change", "audit")
if updated:
    engine.execute("""
        ALTER TABLE audit.feed17change
        ALTER COLUMN director_officer_change_key TYPE integer""")
        
    engine.execute("""
        ALTER TABLE audit.feed17change
        RENAME COLUMN director_officer_change_key TO do_change_key""")
    is_col_to_bool(engine, "audit", "feed17change")

    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON audit.feed17change (do_change_key)")  

updated = wrds_update("feed17del", "audit")

updated = wrds_update("sholderact", "audit")



updated = wrds_update("feed13cat", "audit")

updated = wrds_update("feed14case", "audit")
if updated:
    engine.execute("""
        ALTER TABLE audit.feed14case ALTER COLUMN legal_case_key TYPE integer;
        ALTER TABLE audit.feed14case ALTER COLUMN law_court_key TYPE integer;
        ALTER TABLE audit.feed14case ALTER COLUMN judge_key TYPE integer;
        ALTER TABLE audit.feed14case ALTER COLUMN der_legal_case_fkey TYPE integer;
        ALTER TABLE audit.feed14case ALTER COLUMN lcd_ref_id TYPE integer;

        ALTER TABLE audit.feed14case
            ALTER COLUMN create_date TYPE date USING create_date::date;

        ALTER TABLE audit.feed14case
            ALTER COLUMN change_date TYPE date USING change_date::date;

        ALTER TABLE audit.feed14case
            ALTER COLUMN cross_claim TYPE boolean USING cross_claim=1;

        ALTER TABLE audit.feed14case
            ALTER COLUMN counter_claim TYPE boolean USING counter_claim=1;

        -- ALTER TABLE audit.feed14case
        --    ALTER COLUMN exp_end_date_x TYPE date USING exp_end_date_x::date;

        -- ALTER TABLE audit.feed14case
        --    ALTER COLUMN exp_start_date_x TYPE date USING exp_start_date_x::date;

        ALTER TABLE audit.feed14case
            ALTER COLUMN case_end_date_x TYPE date USING case_end_date_x::date;

        ALTER TABLE audit.feed14case
            ALTER COLUMN case_start_date_x TYPE date USING case_start_date_x::date;

        ALTER TABLE audit.feed14case DROP COLUMN case_start_date_s;
        ALTER TABLE audit.feed14case DROP COLUMN case_end_date_s;
        ALTER TABLE audit.feed14case DROP COLUMN exp_start_date_s;
        ALTER TABLE audit.feed14case DROP COLUMN exp_end_date_s;""")

updated = wrds_update("feed14party", "audit")
if updated:
    engine.execute("""
        ALTER TABLE audit.feed14party ADD COLUMN company_fkey_temp integer;

        UPDATE audit.feed14party SET company_fkey_temp=CASE WHEN company_fkey='.' THEN NULL ELSE company_fkey::integer END;
        ALTER TABLE audit.feed14party DROP COLUMN company_fkey;
        ALTER TABLE audit.feed14party RENAME COLUMN company_fkey_temp TO company_fkey;

        ALTER TABLE audit.feed14party ALTER COLUMN auditor_key TYPE integer;
        ALTER TABLE audit.feed14party ALTER COLUMN gov_key TYPE integer;
        ALTER TABLE audit.feed14party ALTER COLUMN law_firm_key TYPE integer;
        ALTER TABLE audit.feed14party ALTER COLUMN legal_case_key TYPE integer;

        ALTER TABLE audit.feed14party ALTER COLUMN defendant TYPE boolean USING defendant=1;
        ALTER TABLE audit.feed14party ALTER COLUMN plaintiff TYPE boolean USING plaintiff=1;
        ALTER TABLE audit.feed14party ALTER COLUMN is_lead TYPE boolean USING is_lead=1;
        ALTER TABLE audit.feed14party ALTER COLUMN consol TYPE boolean USING consol=1;
        ALTER TABLE audit.feed14party ALTER COLUMN rel_non_party TYPE boolean USING rel_non_party=1;
        ALTER TABLE audit.feed14party ALTER COLUMN rel_defendant TYPE boolean USING rel_defendant=1;
        ALTER TABLE audit.feed14party ALTER COLUMN third_party TYPE boolean USING third_party=1;
        ALTER TABLE audit.feed14party ALTER COLUMN is_debtor TYPE boolean USING is_debtor=1;
        ALTER TABLE audit.feed14party ALTER COLUMN is_creditor TYPE boolean USING is_creditor=1;
        ALTER TABLE audit.feed14party ALTER COLUMN been_terminated TYPE boolean USING been_terminated=1;
    """)



updated = wrds_update("auditchange", "audit", drop="matchfy: matchqu: priorfy: priorqu: closestqu: closestfy:")
if updated:
    is_col_to_bool(engine, "audit", "auditchange")

updated = wrds_update("auditsox404", "audit", drop="closest: match: prior: op_aud_name eventdate_aud_name")
if updated:
    is_col_to_bool(engine, "audit", "auditsox404")

updated = wrds_update("auditsox404", "audit", keep="auditor_fkey op_aud_name",
                      alt_table_name="sox404_auditors")
updated = wrds_update("auditsox404", "audit", keep="eventdate_aud_fkey eventdate_aud_name",
                      alt_table_name="sox404_event_auditors")

# eventdate_aud_name          
updated = wrds_update("auditsox302", "audit", 
                      drop="match: prior: closest: ic_dc_text:")
if updated:
    engine.execute("""
        ALTER TABLE audit.auditsox302
        ALTER COLUMN is_effective TYPE integer USING is_effective::integer""")
    is_col_to_bool(engine, "audit", "auditsox302")

updated = wrds_update("auditlegal", "audit", drop="closest: match: prior:")
if updated:
     # Takes a lot of time
    is_col_to_bool(engine, "audit", "auditlegal")

engine.dispose()
