#!/usr/bin/env python3
from wrds2pg import wrds_update

wrds_update("voteanalysis_npx", "risk")
wrds_update("proposals", "risk")
wrds_update("proposals_legacy", "risk")
