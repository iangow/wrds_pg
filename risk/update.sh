#!/bin/bash
echo "Get 'easy' risk data."
risk/update_issvoting.py

echo "Get 'difficult' risk data."
echo "$PGHOST"
Rscript --vanilla risk/get_risk_data.R

Rscript --vanilla risk/create_rm_link.R
Rscript --vanilla risk/import_manual_names.R
