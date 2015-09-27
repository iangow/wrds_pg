#!/usr/bin/env bash
perl wrds_update.pl --fix-missing audit.diroffichange --drop="match: prior: "
if [ $? -eq 1 ] ; then
    psql -f audit/fix_diroffichange.sql
fi
perl wrds_update.pl --fix-missing audit.namesdiroffichange

perl wrds_update.pl --fix-missing audit.auditnonreli --drop="match: prior: "
perl wrds_update.pl --fix-missing audit.namesauditnonreli

perl wrds_update.pl audit.namesbankrupt
perl wrds_update.pl audit.bankrupt --drop="match: closest: prior:"
if [ $? -eq 1 ] ; then
    psql -f audit/fix_bankrupt.sql
fi

perl wrds_update.pl audit.feed13cat

perl wrds_update.pl audit.feed14case
if [ $? -eq 1 ] ; then
    psql -f audit/fix_feed14case.sql
fi

perl wrds_update.pl audit.feed14party
if [ $? -eq 1 ] ; then
    psql -f audit/fix_feed14party.sql
fi

perl wrds_update.pl audit.feed17person
if [ $? -eq 1 ] ; then
    psql -f audit/fix_feed14person.sql
fi

perl wrds_update.pl audit.feed17change
perl wrds_update.pl audit.feed17del
perl wrds_update.pl audit.sholderact

Rscript audit/get_feed09filing.R
Rscript audit/get_feed09.R
