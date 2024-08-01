#!/usr/bin/env python3
from wrds2pg import wrds_update

# Auditor Changes
updated = wrds_update("feed55_auditor_ratification", "audit", 
                      drop="match: prior: closest: ",
                      col_types={"auditor_ratification_fkey": "integer",
                                 "share_class_fkey": "integer",
                                 "auditor_fkey": "integer",
                                 "pcaob_registration_number": "integer"})

wrds_update("feed56_accounting_estimates_chan", "audit", 
            drop="match: prior: closest:",
            col_types={"accounting_estimates_cha_fke": "integer"})

wrds_update("feed65_impairments", "audit",
            drop="match: prior: closest: material_impairment_text mtrial_impairment_text_html",
            col_types={"mtrl_imprmnt_fct_key": "integer",
                       "mtrl_imprmnt_key": "integer",
                       "quantitative_taxonomy_fkey": "integer",
                       "eventdate_aud_fkey": "integer",
                       "is_range": "boolean",
                       "is_final": "boolean"})

#wrds_update("feed74_aqrm", "audit",
#            drop="match: prior: closest:",
#            col_types={"flag_year": "integer",
#                       "fye_of_opinion": "integer",
#                       "ideal_fye_of_opinion": "integer"})

wrds_update("feed78_critical_audit_matters", "audit", 
            col_types={"audit_opinion_fkey": "integer",
                       "auditor_fkey": "integer",
                       "critical_audit_matter_key": "integer",
                       "critical_audit_matter_topic_fkey": "integer"})

# wrds_update("feed85_cybersecurity", "audit", 
#             col_types={"cybersecurity_breach_key": "integer",
#                       "number_of_records_lost": "integer"},
#             drop="match: prior: closest:")

# wrds_update("feed86_audit_firm_events", "audit",
#            drop="match: prior: closest:")

wrds_update("feed89_pcaob_report", "audit",
            col_types={"auditor_report_key": "integer",
                       "auditor_affiliate_fkey": "integer",
                       "inspection_year": "integer",
                       "auditor_fkey": "integer",
                       "has_firm_signed": "boolean",
                       "has_written_response": "boolean",
                       "is_clean_report": "boolean"})

wrds_update("feed91_aaer", "audit",
            col_types={"aaer_event_key": "integer",
                       "first_release_fkey": "integer",
                       "most_recent_release_fkey": "integer"})
