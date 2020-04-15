#!/usr/bin/env python3
from wrds2pg import wrds_update

dealscan_tables = ["borrowerbase", "company", "currfacpricing", "dealamendment", 
    "dealpurposecomment", "facility", "facilityamendment", "facilitydates", 
    "facilityguarantor", "facilitypaymentschedule", "facilitysecurity", 
    "facilitysponsor", "financialcovenant", "financialratios", "lendershares", 
    "link_table", "lins", "marketsegment", "networthcovenant", "organizationtype", 
    "package", "packageassignmentcomment", "performancepricing", 
    "performancepricingcomments", "sublimits", "dbo_df_fac_dates_data"]

for table in dealscan_tables:
    wrds_update(table, "dealscan")
