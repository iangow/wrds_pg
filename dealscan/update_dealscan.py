#!/usr/bin/env python3
from sqlalchemy import create_engine
from wrds2pg import wrds_update, run_file_sql
import sys
sys.path.insert(0, '..')

from make_engine import engine, wrds_id

wrds_update("borrowerbase", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("company", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("currfacpricing", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("dealamendment", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("dealpurposecomment", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facility", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facilityamendment", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facilitydates", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facilityguarantor", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facilitypaymentschedule", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facilitysecurity", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("facilitysponsor", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("financialcovenant", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("financialratios", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("lendershares", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("link_table", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("lins", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("marketsegment", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("networthcovenant", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("organizationtype", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("package", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("packageassignmentcomment", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("performancepricing", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("performancepricingcomments", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("sublimits", "dealscan", engine=engine, wrds_id=wrds_id)
wrds_update("dbo_df_fac_dates_data", "dealscan", engine=engine, wrds_id=wrds_id)
