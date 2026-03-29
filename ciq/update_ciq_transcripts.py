#!/usr/bin/env python3
from db2pq import wrds_update_pg, wrds_get_tables

table_list = wrds_get_tables("ciq_transcripts")

for table in table_list:
    fix_cr = table in ["ciqtranscriptperson", "wrds_transcript_person"]
    wrds_update(table, "ciq")
