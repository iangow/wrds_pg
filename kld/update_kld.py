#!/usr/bin/env python3
from wrds2pg import wrds_update
import subprocess
import os

updated = wrds_update("history", "kld", fix_missing=True)
if updated:
    print("Creating 'long' data table.")
    print(os.getenv("PGHOST"))
    subprocess.call (["Rscript", "--vanilla", "kld/create_history_long.R"])
    
updated = wrds_update("kldnames", "kld", fix_missing=True)
