#!/usr/bin/env python3
from sqlalchemy import create_engine
from wrds2pg import wrds_update, run_file_sql
import sys
sys.path.insert(0, '..')

from make_engine import engine, wrds_id

wrds_update("globalvoteresults", "risk", engine=engine, wrds_id=wrds_id)
wrds_update("vavoteresults", "risk", engine=engine, wrds_id=wrds_id)
wrds_update("issrec", "risk", engine=engine, wrds_id=wrds_id)
