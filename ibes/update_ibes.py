#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds2pg import wrds2pg
from time import gmtime, strftime

wrds2pg.wrds_update("act_epsus", "ibes")
wrds2pg.wrds_update("actpsumu_epsus", "ibes")
wrds2pg.wrds_update("actu_epsus", "ibes")
updated = wrds2pg.wrds_update("detu_epsus", "ibes")
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON ibes.detu_epsus (ticker, revdats)")

updated = wrds2pg.wrds_update("det_xepsus", "ibes")
if updated:
    engine.execute("CREATE INDEX ON ibes.det_xepsus (ticker, revdats)")
    
wrds2pg.wrds_update("det_epsus", "ibes")
wrds2pg.wrds_update("id", "ibes")
wrds2pg.wrds_update("idsum", "ibes")
wrds2pg.wrds_update("surpsum", "ibes")
wrds2pg.wrds_update("statsum_epsus", "ibes")
updated = wrds2pg.wrds_update("statsumu_epsus", "ibes")
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON ibes.statsumu_epsus (ticker, statpers)")

# Update guidance
wrds2pg.wrds_update("det_guidance", "ibes")
wrds2pg.wrds_update("det_guidance_ext", "ibes")
wrds2pg.wrds_update("id_guidance", "ibes")
wrds2pg.wrds_update("split_guidance", "ibes")

wrds2pg.wrds_update("stop_epsus", "ibes")
wrds2pg.wrds_update("exc_epsus", "ibes")
wrds2pg.wrds_update("actpsum_epsus", "ibes")

# Rscript ibes/get_iclink.R
engine.execute("GRANT USAGE ON SCHEMA ibes TO wrds")
engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ibes TO wrds")
