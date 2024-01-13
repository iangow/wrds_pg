#!/usr/bin/env python3
from wrds2pg import wrds_update, run_file_sql, make_engine

engine = make_engine()

wrds_update("act_epsus", "ibes")
wrds_update("actpsumu_epsus", "ibes")
wrds_update("actu_epsus", "ibes")
updated = wrds_update("detu_epsus", "ibes")
if updated:
     process_sql("CREATE INDEX ON ibes.detu_epsus (ticker, revdats)", engine=engine)
    
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

updated = wrds_update("surpsumu", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.surpsumu (ticker)", engine=engine)

wrds_update("statsum_epsus", "ibes")
updated = wrds_update("statsumu_epsus", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.statsumu_epsus (ticker, statpers)", engine=engine)

wrds_update("det_guidance", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.det_guidance (anndats);", engine=engine)

    
wrds_update("det_guidance_ext", "ibes")
wrds_update("id_guidance", "ibes")
wrds_update("split_guidance", "ibes")
wrds_update("stop_epsus", "ibes")
wrds_update("exc_epsus", "ibes")
wrds_update("actpsum_epsus", "ibes")
wrds_update("actpsum_epsint", "ibes")
wrds_update("statsum_epsint", "ibes")

engine.dispose()

import subprocess
subprocess.call("Rscript --vanilla ibes/get_iclink.R", shell=True)
