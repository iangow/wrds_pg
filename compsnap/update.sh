#!/usr/bin/env bash
./wrds_update.pl compsnap.wrds_csq_pit

if [ $? -eq 1 ] ; then
    psql -f compsnap/make_indexes.sql
fi

./wrds_update.pl compsnap.wrds_csa_pit

if [ $? -eq 1 ] ; then
    psql -f compsnap/make_indexes_a.sql
fi

./wrds_update.pl compsnap.cscompany

