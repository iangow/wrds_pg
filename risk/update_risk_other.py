#!/usr/bin/env python3
from db2pq import wrds_update_pg

wrds_update_pg("va_proposals", "risk")
wrds_update_pg("issrec", "risk")
wrds_update_pg("voteanalysis_npx", "risk")
wrds_update_pg("proposals", "risk")
wrds_update_pg("proposals_legacy", "risk")
