#!/usr/bin/env python3
from wrds2pg.wrds2pg import wrds_update

wrds_update("wciklink_gvkey", "wrdssec")
wrds_update("wciklink_names", "wrdssec")
wrds_update("wciklink_cusip", "wrdssec", drop="tmatch")
