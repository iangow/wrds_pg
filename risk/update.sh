#!/bin/bash
echo "Get 'easy' risk data."
risk/update_issvoting.py

echo "Get 'difficult' risk data."
echo "$PGHOST"
Rscript --vanilla risk/get_risk_data.R
