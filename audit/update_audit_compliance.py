#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine, process_sql

engine = make_engine()

# Auditors
updated = wrds_update("feed01_auditors", "audit", 
                      col_types={"auditor_key": "integer",
                                   "pcaob_reg_num": "integer",
                                   "pcaob_app_num": "integer",
                                   "aud_loc_key": "integer", 
                                   "eventdate_aud_fkey": "integer", 
                                   "auditor_pcaob_reg": "boolean"})

# Auditor Changes
updated = wrds_update("feed02_auditor_changes", "audit", 
                      col_types={"auditor_change_key": "integer",
                                   "dismiss_key": "integer",
                                   "file_accepted": "timestamptz",
                                   "engaged_auditor_key": "integer",
                                   "dismissed_gc": "boolean", 
                                   "dismissed_disagree": "boolean", 
                                   "auditor_resigned": "boolean",
                                   "dismiss_pcaob_reg": "boolean",
                                   "merger": "boolean",
                                   "is_benefit_plan": "boolean",
                                   "aud_letter_disagree": "boolean", 
                                   "aud_letter_no_comm": "boolean",
                                   "aud_letter_agree": "boolean",
                                   "aud_co_disagree": "boolean",
                                   "engaged_auditor_pcaob": "boolean"}, 
                      drop="match: prior: closest: dismiss_name " + 
                           "engaged_auditor_name eventdate_aud_name",
                      tz="America/New_York")

# Audit Fees
updated = wrds_update("feed03_audit_fees", "audit", 
                      drop="match: prior: closest: auditor_name eventdate_aud_name", 
                      col_types={"eventdate_aud_fkey": "integer",
                                   "file_accepted": "timestamptz",
                                   "auditor_fkey": "integer", 
                                   "audit_gig_key": "integer",
                                   "fiscal_year": "integer",
                                   "restatement":"boolean",
                                   "fees_pcaob_reg":"boolean",
                                   "is_benefit_plan":"boolean"},
                      tz="America/New_York")
               
# Audit Fees with Restatements
updated = wrds_update("feed04_audit_fees_restated", "audit", 
                      drop="match: prior: closest: auditor_name eventdate_aud_name", 
                      col_types={"eventdate_aud_fkey": "integer", 
                                   "file_accepted": "timestamptz",
                                   "auditor_fkey": "integer", 
                                   "audit_gig_key": "integer",
                                   "fiscal_year": "integer",
                                   "restatement":"boolean",
                                   "fees_pcaob_reg":"boolean",
                                   "is_benefit_plan":"boolean"},
                      tz="America/New_York")

updated = wrds_update("feed06_benefit_plan_opinions", "audit", 
                      drop="match: prior: closest:",
                      col_types={"benefit_plan_key": "integer", 
                                   "auditor_fkey": "integer",
                                   "auditor_affil_fkey": "integer",
                                   "is_nth_opinion": "integer",
                                   "op_aud_pcaob": "boolean",
                                   "going_concern": "boolean",
                                   "eventdate_aud_fkey": "integer",
                                   "fiscal_year_of_op": "integer"})

wrds_update("feed07_current_auditor", "audit", 
            col_types={"auditor_key": "integer"}) 

wrds_update("feed08_auditor_during", "audit", 
            col_types={"auditor_fkey": "integer"}) 

# SOX 302 Disclosure Controls
wrds_update("feed10_sox_302_disclosure_contro", "audit",
            drop="prior: match: closest: ic_dc_text:",
            col_types={"ic_dc_key": "integer", 
                       "is_effective": "integer",
                       "material_weakness": "boolean",
                       "sig_deficiency": "boolean",
                       "noteff_acc_rule": "integer",
                       "noteff_fin_fraud": "integer",
                       "notefferrors": "integer",
                       "noteff_other": "integer",
                       "eventdate_aud_fkey": "integer"})

#wrds_update("feed10_sox_302_disclosure_contro", "audit", 
#            keep="ic_dc_key ic_dc_text:",
#            alt_table_name="feed10_sox_302_disclosure_contro_text",
#            col_types={"ic_dc_key": "integer"})
                     
# SOX 404 Internal Controls
updated = wrds_update("feed11_sox_404_internal_controls", "audit",
                      drop="prior: match: closest: ic_text:",
                        col_types={"ic_op_fkey": "integer",
                                     "auditor_fkey": "integer", 
                                     "eventdate_aud_fkey": "integer"})

# Accelerated Filer
updated = wrds_update("feed16_accelerated_filer", "audit",
                      drop="prior: match: closest:",
                      col_types={"accel_filer_key": "integer",
                                   "hst_season_issuer": "integer",   
                                   "hst_is_shell_co": "integer",                      
                                   "hst_is_accel_filer": "integer",     
                                   "hst_is_large_accel": "integer", 
                                   "hst_is_voluntary_filer": "integer", 
                                   "hst_is_small_report": "integer",   
                                   "did_not_disc": 'boolean',
                                   "file_accepted": "timestamptz",
                                   "eventdate_aud_fkey": "integer"},
                      tz="America/New_York")

# Director and officer changes
updated = wrds_update("feed17_director_and_officer_chan", "audit",
                        drop="match: prior: closest: do_change_text:",
                        col_types={"do_off_pers_key": "integer",
                                     "do_change_key": "integer",
                                     "eventdate_aud_fkey": "integer",
                                     "file_accepted": "timestamptz",
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
                                     'is_exec_vp': 'boolean'},
                      tz="America/New_York")

if updated:                     
    process_sql("CREATE INDEX ON audit.feed17_director_and_officer_chan (do_off_pers_key)",
                engine)

# Non-timely Filer Information And Analysis
updated = wrds_update("feed20_nt", "audit",
                      drop="match: closest: prior: ",
                      col_types={"nt_notify_key": "integer",
                                 "ac_file_accepted": "timestamptz",
                                 "eventdate_aud_fkey": "integer",
                                 "aud_at_file_date": "integer",
                                 "part2_c_check": "boolean", 
                                 "part2_b_check": "boolean",
                                 "part2_a_check": "boolean",
                                 "part4_3_check": "boolean",
                                 "file_accepted": "timestamptz",
                                 "ten_k_trans_report": "boolean"},
                      tz="America/New_York")
engine.dispose()

updated = wrds_update("feed34_revised_audit_opinions", "audit",
                      drop="match: prior: closest:",
                      col_types={"audit_op_key": "integer", 
                                   "eventdate_aud_fkey": "integer",
                                   "integrated_audit": "boolean",
                                   "auditor_fkey": "integer",
                                   "auditor_affil_fkey": "integer",
                                   "is_nth_add_op": "integer",
                                   "going_concern": "boolean",
                                   "op_aud_pcaob": "boolean",
                                   "file_accepted": "timestamptz",
                                   "eventdate_aud_fkey": "integer",
                                   "fiscal_year_of_op": "integer"},
                      tz="America/New_York")
