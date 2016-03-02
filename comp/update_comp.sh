#!/usr/bin/env bash
./wrds_update.pl comp.anncomp
./wrds_update.pl comp.adsprate
./wrds_update.pl comp.co_hgic
./wrds_update.pl comp.co_ifndq
./wrds_update.pl comp.company
./wrds_update.pl comp.idx_ann
./wrds_update.pl comp.idx_index
./wrds_update.pl comp.io_qbuysell
./wrds_update.pl comp.names
./wrds_update.pl comp.secm
./wrds_update.pl comp.wrds_segmerged
./wrds_update.pl comp.spind_mth
./wrds_update.pl comp.funda --fix-missing
./wrds_update.pl comp.fundq --fix-missing
./wrds_update.pl comp.g_sec_divid
./wrds_update.pl comp.idxcst_his --rename=from=fromdt
psql < comp/create_ciks.sql
psql < comp/comp_indexes.sql
psql < pg/permissions.sql

pg_dump --format custom --no-tablespaces --verbose --file \
    $PGBACKUP_DIR/comp.backup --schema "comp" "crsp"
