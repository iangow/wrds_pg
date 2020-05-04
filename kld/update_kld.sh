#!/usr/bin/env bash

echo "Getting new data"
kld/import_kld.py

echo "Creating 'long' data table."
echo "$PGHOST"
Rscript --vanilla kld/create_history_long.R
