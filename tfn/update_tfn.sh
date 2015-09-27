#!/usr/bin/env bash
./wrds_update.pl tfn.amend
./wrds_update.pl tfn.avgreturns
./wrds_update.pl tfn.rule10b5
./wrds_update.pl tfn.table1
./wrds_update.pl tfn.table2
./wrds_update.pl tfn.s12
./wrds_update.pl tfn.s12type1
./wrds_update.pl tfn.s12type2
./wrds_update.pl tfn.s34
./wrds_update.pl tfn.s34type1
./wrds_update.pl tfn.s34type2
if [ $? -eq 1 ] ; then
    pg_dump  --format custom --no-tablespaces --verbose \
        --file $PGBACKUP_DIR/tfn.backup --schema "tfn"
fi
