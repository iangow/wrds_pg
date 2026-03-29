#!/bin/bash
echo "$PGHOST"
echo "$PGPORT"

uv run python comp/update_comp.py

Rscript --vanilla comp/create_gvkey_cik.R
Rscript --vanilla comp/create_ncusips.R
