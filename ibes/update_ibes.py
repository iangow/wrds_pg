#!/usr/bin/env python3
from sqlalchemy import create_engine
from wrds2pg import wrds_update, run_file_sql
import sys
sys.path.insert(0, '..')

from make_engine import engine, wrds_id

wrds_update("act_epsus", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("actpsumu_epsus", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("actu_epsus", "ibes", engine=engine, wrds_id=wrds_id)
updated = wrds_update("detu_epsus", "ibes", engine=engine, wrds_id=wrds_id)
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON ibes.detu_epsus (ticker, revdats)")

updated = wrds_update("det_xepsus", "ibes", engine, wrds_id)
if updated:
    engine.execute("CREATE INDEX ON ibes.det_xepsus (ticker, revdats)")
    
wrds_update("det_epsus", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("id", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("idsum", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("surpsum", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("statsum_epsus", "ibes", engine=engine, wrds_id=wrds_id)
updated = wrds_update("statsumu_epsus", "ibes", engine=engine, wrds_id=wrds_id)
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON ibes.statsumu_epsus (ticker, statpers)")

# Update guidance
wrds_update("det_guidance", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("det_guidance_ext", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("id_guidance", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("split_guidance", "ibes", engine=engine, wrds_id=wrds_id)

wrds_update("stop_epsus", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("exc_epsus", "ibes", engine=engine, wrds_id=wrds_id)
wrds_update("actpsum_epsus", "ibes", engine=engine, wrds_id=wrds_id)

# Rscript ibes/get_iclink.R
engine.execute("GRANT USAGE ON SCHEMA ibes TO wrds")
engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ibes TO wrds")
