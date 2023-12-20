#!/usr/bin/env python3
from wrds2pg import wrds_update, run_file_sql
from wrds2pg import make_engine, wrds_id

engine = make_engine()

wrds_update("act_epsus", "ibes")
wrds_update("actpsumu_epsus", "ibes")
wrds_update("actu_epsus", "ibes")
updated = wrds_update("detu_epsus", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.detu_epsus (ticker, revdats)", 
                engine=engine)
    
updated = wrds_update("det_xepsus", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.det_xepsus (ticker, revdats)",
                engine=engine)

wrds_update("curr", "ibes")    
wrds_update("det_epsus", "ibes")
wrds_update("detu_epsint", "ibes")
wrds_update("id", "ibes")
wrds_update("idsum", "ibes")
wrds_update("surpsum", "ibes")
wrds_update("statsum_epsus", "ibes")
updated = wrds_update("statsumu_epsus", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.statsumu_epsus (ticker, statpers)",
                engine=engine)

wrds_update("det_guidance", "ibes")
if updated:
    engine.execute("SET maintenance_work_mem='1999MB'")
    process_sql("CREATE INDEX ON ibes.det_guidance (anndats)",
                engine=engine)
    
wrds_update("det_guidance_ext", "ibes")
wrds_update("id_guidance", "ibes")
wrds_update("split_guidance", "ibes")
wrds_update("stop_epsus", "ibes")
wrds_update("exc_epsus", "ibes")
wrds_update("actpsum_epsus", "ibes")

engine.dispose()

import subprocess
subprocess.call("Rscript --vanilla ibes/get_iclink.R", shell=True)
