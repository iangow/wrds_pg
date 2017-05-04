#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
dbname = engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update

updated = wrds_update("anncomp", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.anncomp (gvkey)")

updated = wrds_update("adsprate", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.adsprate (gvkey, datadate)")

updated = wrds_update("co_hgic", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.co_hgic (gvkey)")

updated = wrds_update("co_ifndq", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.co_ifndq (gvkey, datadate)")

company_updated = wrds_update("company", "comp", engine, wrds_id)
if company_updated:
    engine.execute("CREATE INDEX ON comp.company (gvkey)")

updated = wrds_update("idx_ann", "comp", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON comp.idx_ann (datadate)")

updated = wrds_update("idx_index", "comp", engine, wrds_id)

