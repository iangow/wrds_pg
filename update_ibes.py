#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
dbname = engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds_fetch import wrds_update

wrds_update("act_epsus", "ibes", engine, wrds_id)
wrds_update("actpsumu_epsus", "ibes", engine, wrds_id)
wrds_update("actu_epsus", "ibes", engine, wrds_id)
updated = wrds_update("detu_epsus", "ibes", engine, wrds_id)
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON ibes.detu_epsus (ticker, revdats)")

wrds_update("det_epsus", "ibes", engine, wrds_id)
wrds_update("id", "ibes", engine, wrds_id)
wrds_update("idsum", "ibes", engine, wrds_id)
wrds_update("surpsum", "ibes", engine, wrds_id)
wrds_update("statsum_epsus", "ibes", engine, wrds_id)
updated = wrds_update("statsumu_epsus", "ibes", engine, wrds_id)
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON ibes.statsumu_epsus (ticker, statpers)")

# Update guidance
wrds_update("det_guidance", "ibes", engine, wrds_id)
wrds_update("det_guidance_ext", "ibes", engine, wrds_id)
wrds_update("id_guidance", "ibes", engine, wrds_id)
wrds_update("split_guidance", "ibes", engine, wrds_id)

# Rscript ibes/get_iclink.R
engine.execute("GRANT USAGE ON SCHEMA ibes TO wrds")
engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ibes TO wrds")
