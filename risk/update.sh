#!/bin/bash
echo "$PGHOST"
echo "$PGPORT"

risk/update_issvoting.py

Rscript --vanilla risk/create_rm_link.R
Rscript --vanilla risk/import_manual_names.R
