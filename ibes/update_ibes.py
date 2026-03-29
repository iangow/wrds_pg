#!/usr/bin/env python3
from db2pq import wrds_update_pg, process_sql

wrds_update_pg("act_epsus", "ibes", col_types={"acttims": "string", "anntims": "string"})
wrds_update_pg("act_xepsus", "ibes")
wrds_update_pg("actpsumu_epsus", "ibes")
wrds_update_pg("actu_epsus", "ibes")
updated = wrds_update_pg("detu_epsus", "ibes")
if updated:
     process_sql("CREATE INDEX ON ibes.detu_epsus (ticker, revdats)")

updated = wrds_update_pg("det_xepsus", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.det_xepsus (ticker, revdats)")

wrds_update_pg("curr", "ibes")
wrds_update_pg("det_epsus", "ibes")
wrds_update_pg("detu_epsint", "ibes")
wrds_update_pg("id", "ibes")
wrds_update_pg("idsum", "ibes")
wrds_update_pg("surpsum", "ibes")

updated = wrds_update_pg("surpsumu", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.surpsumu (ticker)")

# wrds_update_pg("actsum_epsint", "ibes")
# wrds_update_pg("actsum_xepsint", "ibes")
wrds_update_pg("statsum_epsus", "ibes")
updated = wrds_update_pg("statsumu_epsus", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.statsumu_epsus (ticker, statpers)")

updated = wrds_update_pg("det_guidance", "ibes")
if updated:
    process_sql("CREATE INDEX ON ibes.det_guidance (anndats);")

wrds_update_pg("det_guidance_ext", "ibes", use_sas=True)
wrds_update_pg("id_guidance", "ibes", use_sas=True)
wrds_update_pg("split_guidance", "ibes", use_sas=True)
wrds_update_pg("stop_epsus", "ibes")
wrds_update_pg("exc_epsus", "ibes")
wrds_update_pg("actpsum_epsus", "ibes")
wrds_update_pg("actpsum_epsint", "ibes")
wrds_update_pg("actpsum_xepsint", "ibes")
wrds_update_pg("statsum_epsint", "ibes")
wrds_update_pg("statsum_xepsint", "ibes")

#import subprocess
#subprocess.call("Rscript --vanilla ibes/get_iclink.R", shell=True)
