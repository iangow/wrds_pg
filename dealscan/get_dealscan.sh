#!/usr/bin/env bash
./wrds_update.pl dealscan.borrowerbase
./wrds_update.pl dealscan.company
./wrds_update.pl dealscan.currfacpricing
./wrds_update.pl dealscan.dealamendment
./wrds_update.pl dealscan.dealpurposecomment
./wrds_update.pl dealscan.facility
./wrds_update.pl dealscan.facilityamendment
./wrds_update.pl dealscan.facilitydates
./wrds_update.pl dealscan.facilityguarantor
./wrds_update.pl dealscan.facilitypaymentschedule
./wrds_update.pl dealscan.facilitysecurity
./wrds_update.pl dealscan.facilitysponsor
./wrds_update.pl dealscan.financialcovenant
./wrds_update.pl dealscan.financialratios
./wrds_update.pl dealscan.lendershares
./wrds_update.pl dealscan.link_table
./wrds_update.pl dealscan.lins
./wrds_update.pl dealscan.marketsegment
./wrds_update.pl dealscan.networthcovenant
./wrds_update.pl dealscan.organizationtype
./wrds_update.pl dealscan.package
./wrds_update.pl dealscan.packageassignmentcomment
./wrds_update.pl dealscan.packageprepaymentcomment
./wrds_update.pl dealscan.performancepricing
./wrds_update.pl dealscan.performancepricingcomments
./wrds_update.pl dealscan.sublimits
./wrds_update.pl dealscan.link_table
./wrds_update.pl dealscan.link_table
./wrds_update.pl dealscan.dbo_df_fac_dates_data

pg_dump --format custom --no-tablespaces --verbose \
    --file $PGBACKUP_DIR/dealscan.backup --schema "dealscan" "crsp"
