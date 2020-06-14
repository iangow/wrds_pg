#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine

engine = make_engine()

# Legal Case And Legal Parties
updated = wrds_update("auditlegal", "audit", 
                      drop="closest: match: prior: ",
                      col_types = { "legal_party_key":"integer",
                                    "auditor_key":"integer",
                                    "gov_key":"integer",
                                    "law_firm_key":"integer",
                                    "legal_case_key":"integer",
                                    "is_creditor": "boolean",
                                    "is_debtor": "boolean", 
                                    "is_non_party_par": "boolean",   
                                    "is_nom_defendant": "boolean", 
                                    "is_pcaob_reg": "boolean", 
                                    "is_active": "boolean", 
                                    "is_category_type_1": "boolean",
                                    "is_category_type_2": "boolean",
                                    "is_category_type_3": "boolean",
                                    "is_category_type_4": "boolean",
                                    "is_category_type_5": "boolean",
                                    "is_category_type_6": "boolean",
                                    "is_category_type_7": "boolean",
                                    "is_category_type_8": "boolean",
                                    "is_category_type_9": "boolean",
                                    "is_category_type_10": "boolean",
                                    "is_category_type_11": "boolean",
                                    "is_category_type_12": "boolean",
                                    "is_category_type_13": "boolean",
                                    "is_category_type_14": "boolean",
                                    "is_category_type_15": "boolean",
                                    "is_category_type_16": "boolean",
                                    "is_category_type_17": "boolean",
                                    "is_category_type_18": "boolean",
                                    "is_category_type_19": "boolean",
                                    "is_category_type_20": "boolean",
                                    "is_category_type_21": "boolean",
                                    "is_category_type_22": "boolean",
                                    "is_category_type_23": "boolean",
                                    "is_category_type_24": "boolean",
                                    "is_category_type_25": "boolean",
                                    "is_category_type_26": "boolean",
                                    "is_category_type_28": "boolean",
                                    "is_category_type_29": "boolean",
                                    "is_category_type_30": "boolean",
                                    "is_category_type_31": "boolean",
                                    "is_category_type_32": "boolean",
                                    "is_category_type_33": "boolean",
                                    "is_category_type_34": "boolean",
                                    "is_category_type_35": "boolean",
                                    "is_category_type_36": "boolean",
                                    "is_category_type_37": "boolean",
                                    "is_category_type_38": "boolean",
                                    "is_category_type_39": "boolean",
                                    "is_category_type_40": "boolean",
                                    "is_category_type_41": "boolean",
                                    "is_category_type_42": "boolean",
                                    "is_category_type_43": "boolean",
                                    "is_category_type_44": "boolean",
                                    "is_category_type_48": "boolean",
                                    "is_category_type_49": "boolean",
                                    "is_category_type_50": "boolean",
                                    "is_category_type_51": "boolean",
                                    "is_category_type_52": "boolean",
                                    "is_category_type_53": "boolean",
                                    "is_category_type_54": "boolean",
                                    "is_category_type_55": "boolean",
                                    "is_category_type_56": "boolean",
                                    "is_category_type_57": "boolean",
                                    "is_category_type_58": "boolean",
                                    "is_category_type_59": "boolean",
                                    "is_category_type_60": "boolean",
                                    "is_category_type_61": "boolean",
                                    "is_category_type_62": "boolean",
                                    "is_category_type_63": "boolean",
                                    "is_category_type_64": "boolean",
                                    "is_category_type_66": "boolean",
                                    "is_category_type_67": "boolean",
                                    "is_category_type_68": "boolean",
                                    "is_category_type_69": "boolean",
                                    "is_category_type_70": "boolean",
                                    "is_category_type_71": "boolean",
                                    "is_category_type_72": "boolean",
                                    "is_category_type_73": "boolean",
                                    "is_category_type_74": "boolean",
                                    "is_category_type_75": "boolean",
                                    "is_category_type_76": "boolean",
                                    "is_category_type_77": "boolean",
                                    "is_category_type_78": "boolean",
                                    "is_category_type_79": "boolean",
                                    "is_category_type_80": "boolean",
                                    "is_category_type_81": "boolean",
                                    "is_category_type_82": "boolean",
                                    "is_category_type_83": "boolean",
                                    "is_category_type_84": "boolean",
                                    "is_category_type_85": "boolean",
                                    "is_category_type_86": "boolean",
                                    "is_category_type_87": "boolean",
                                    "is_category_type_88": "boolean",
                                    "is_category_type_89": "boolean",
                                    "is_category_type_90": "boolean",
                                    "is_category_type_91": "boolean",
                                    "is_category_type_92": "boolean",
                                    "is_category_type_93": "boolean",
                                    "is_category_type_94": "boolean",
                                    "is_category_type_95": "boolean",
                                    "is_category_type_96": "boolean",
                                    "is_category_type_97": "boolean",
                                    "is_category_type_98": "boolean",
                                    "is_category_type_99": "boolean",
                                    "is_category_type_100": "boolean",
                                    "is_category_type_101": "boolean",
                                    "is_category_type_102": "boolean",
                                    "is_category_type_103": "boolean",
                                    "is_category_type_105": "boolean",
                                    "is_category_type_106": "boolean",
                                    "is_category_type_107": "boolean",
                                    "is_category_type_108": "boolean",
                                    "is_category_type_109": "boolean",
                                    "defendant":"boolean",   
                                    "plaintiff":"boolean",
                                    "is_lead":"boolean",         
                                    "been_terminated":"boolean",
                                    "consol":"boolean",
                                    "rel_non_party":"boolean",
                                    "rel_defendant":"boolean",
                                    "third_party":"boolean"})

# Mergers and Acquisitions
updated = wrds_update("feed18_merger_acquisition", "audit",
                      col_types = {"ma_party_key": "integer",
                                    "party_co_fkey": "integer",
                                    "party_aud_fkey": "integer",
                                    "m_a_key": "integer",
                                    "ma_transaction_type_fkey": "integer",
                                    "eventdate_aud_fkey": "integer"},
                      drop="closest: match: prior: " + 
                            "comm_date_str trans_date_str")

if updated:
    col = "file_date_list"
    engine.execute("""
            ALTER TABLE audit.feed18_merger_acquisition
            ALTER COLUMN %s TYPE date[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::date[] """ %
                     (col, col))

    for col in ["ftp_file_fkey_list", "form_fkey_list"]:
        engine.execute("""
            ALTER TABLE audit.feed18_merger_acquisition
            ALTER COLUMN %s TYPE text[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::text[] """ %
                     (col, col))

    col = "m_a_filings_keys_list"
    engine.execute("""
        ALTER TABLE audit.feed18_merger_acquisition
        ALTER COLUMN %s TYPE integer[] USING 
            array_remove(string_to_array(%s, '|', ''), NULL)::integer[] """ %
                     (col, col))

# IPO
updated = wrds_update("ipo", "audit",
                      drop="closest: match: prior: ")

# Bankruptcy Notification
updated = wrds_update("bankrupt", "audit",
                      drop="closest: match: prior: ")

# Comment Letter
updated = wrds_update("commlett", "audit", 
                      col_types = {"pub_doc_count":"text"},
                      drop="closest: " + 
                            "cl_text cl_frmt_text_html")

# Comment Letter Conversations
updated = wrds_update("commlettconv", "audit", 
                      drop="closest: ")

# Comment Threading
updated = wrds_update("commlettthreads", "audit", 
                      drop="match: closest: prior: " +
                           "question_text_formatted question_text_html " + 
                           "answer_text_formatted answer_text_html")
if updated:
    list_cols = ["iss_accrl_disc_text","iss_dcic_text", "iss_etifgaap_text",
                  "iss_evnt_disc_text", "iss_fedsec_text", "iss_finguide_text", 
                  "iss_fsp_text", "iss_ftb_text", "iss_ias_text", "iss_ifric_text",
                  "iss_ifrs_text", "iss_legmat_text", "iss_man_disc_text", 
                  "iss_othrdisc_text", "iss_regstatem_text", "iss_regma_text", 
                  "iss_regsksec_text", "iss_regsx_text", "iss_riskfact_text", 
                  "iss_sabguide_text", "iss_sfas_stand_text", "iss_sic_ref_text", 
                  "iss_sop_text", "iss_tendoff_text", "iss_wholet_text", 
                  "iss_fasb_acc_stds_updt_text", "iss_fasb_concpt_stmt_text", 
                  "iss_pcaob_rules_text", "iss_regab_text", "iss_secrts_act_text",
                  "iss_sec_text"]
    for col in list_cols:
        print("Fixing column %s" % col)
        engine.execute("""
            ALTER TABLE audit.commlett
            ALTER COLUMN %s TYPE text[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::text[] """ % (col, col))

    list_cols = ["iss_accrl_disc_keys", "iss_dcic_keys", "iss_etifgaap_keys",
                  "iss_evnt_disc_keys", "iss_fedsec_keys", "iss_finguide_keys", 
                  "iss_fsp_keys", "iss_ftb_keys", "iss_ias_keys", "iss_ifric_keys",
                  "iss_ifrs_keys", "iss_legmat_keys", "iss_man_disc_keys", 
                  "iss_othrdisc_keys", "iss_regstatem_keys", "iss_regma_keys", 
                  "iss_regsksec_keys", "iss_regsx_keys", "iss_riskfact_keys", 
                  "iss_sabguide_keys", "iss_sfas_stand_keys", "iss_sic_ref_keys", 
                  "iss_sop_keys", "iss_tendoff_keys", "iss_wholet_keys", 
                  "iss_fasb_acc_stds_updt_keys", "iss_fasb_concpt_stmt_keys", 
                  "iss_pcaob_rules_keys", "iss_regab_keys", "iss_secrts_act_keys",
                  "iss_sec_keys"]
    for col in list_cols:
        print("Fixing column %s" % col)
        engine.execute("""
            ALTER TABLE audit.commlett
            ALTER COLUMN %s TYPE integer[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::integer[] """ %
                    (col, col)) 

# Transfer Agents
updated = wrds_update("feed41_transfer_agents", "audit")

# Tax Footnotes
updated = wrds_update("taxfootnt", "audit", 
                        col_types = {"tax_footnote_key": "integer",
                                    "times_restated": "integer",
                                    "res_notif_fkey": "integer",
                                    "eventdate_aud_fkey": "integer",
                                    "file_date": "date"},
                        drop="closest: match: prior: period_ended_str")

# Shareholder Activism
updated = wrds_update("sholderact", "audit", 
                        drop="closest: match: prior: " +
                             "iss_file_date_str rep_file_date_str",
                        col_types = {'active_share_key':'integer'})
if updated:
    list_cols = ["agree_keys", "concerns_keys", "control_keys", "disc_keys", 
                 "dispute_keys", "other_keys", "support_keys"]

    for col in list_cols:
        engine.execute("""
            ALTER TABLE audit.sholderact
            ALTER COLUMN %s TYPE integer[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::integer[] """ %
                    (col, col))
                    
    list_cols = ["agree_text", "concerns_text", "control_text", "disctext", 
                "dispute_text", "other_text", "support_text"]

    for col in list_cols:
        engine.execute("""
            ALTER TABLE audit.sholderact
            ALTER COLUMN %s TYPE text[] USING 
                array_remove(string_to_array(%s, '|', ''), NULL)::text[] """ %
                     (col, col))

# Form D
updated = wrds_update("formd", "audit", 
                      drop = "primary_issuer_pre_nam_lis " +
                               "primary_issuer_edg_pre_nam_lis",
                      col_types = {"form_d_key": "integer", 
                                      "is_primary": "boolean"})

# Form D Most Recent Report
updated = wrds_update("formdmro", "audit",
                      drop = "primary_issuer_pre_nam_lis " +
                               "primary_issuer_edg_pre_nam_lis",
                      col_types = {"form_d_key": "integer", 
                                      "is_primary": "boolean"})

