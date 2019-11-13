#!/usr/bin/env bash
cd kld

echo "Getting new data"
./import_kld.py

echo "Creating 'long' data table."
echo "$PGHOST"
Rscript --vanilla ./create_history_long.R
cd ..
