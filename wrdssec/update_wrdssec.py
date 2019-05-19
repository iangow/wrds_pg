#!/usr/bin/env python3
from wrds2pg.wrds2pg import wrds_update

updated = wrds_update("wciklink_gvkey", "wrdssec")
updated = wrds_update("wciklink_names", "wrdssec")
updated = wrds_update("wciklink_cusip", "wrdssec", drop="tmatch")
