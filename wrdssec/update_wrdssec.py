#!/usr/bin/env python3
from sqlalchemy import create_engine
from wrds2pg import wrds_update, run_file_sql
import sys
sys.path.insert(0, '..')

from make_engine import engine, wrds_id

updated = wrds_update("wciklink_gvkey", "wrdssec", engine=engine, wrds_id=wrds_id)
updated = wrds_update("wciklink_names", "wrdssec", engine=engine, wrds_id=wrds_id)
updated = wrds_update("wciklink_cusip", "wrdssec", engine=engine, wrds_id=wrds_id, drop="tmatch")

