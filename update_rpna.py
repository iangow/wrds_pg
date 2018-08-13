#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
dbname = engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update

updated = wrds_update("pr_equities", "rpna", engine, wrds_id)
