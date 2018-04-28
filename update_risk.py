#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
dbname = engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update

wrds_update("globalvoteresults", "risk", engine, wrds_id)
wrds_update("vavoteresults", "risk", engine, wrds_id)
wrds_update("issrec", "risk", engine, wrds_id)
