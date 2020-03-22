#!/usr/bin/env python3
from wrds2pg.wrds2pg import wrds_update

wrds_update("voteanalysis_npx", "risk", obs=100000000, force=True)
# wrds_update("globalvoteresults", "risk")
# wrds_update("vavoteresults", "risk")
# wrds_update("issrec", "risk")
# wrds_update("rmdirectors", "risk")
# wrds_update("gset", "risk")
# wrds_update("votes", "risk")
# wrds_update("voteanalysis_npx", "risk")
# wrds_update("directors", "risk", drop="votecref")
