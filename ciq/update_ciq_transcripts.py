#!/usr/bin/env python3
from wrds2pg import wrds_update, wrds_id, get_wrds_tables

table_list = get_wrds_tables("ciq_transcripts", wrds_id)

for table in table_list:
    fix_cr = table in ["ciqtranscriptperson", "wrds_transcript_person"]
    wrds_update(table, "ciq", fix_cr=fix_cr)
