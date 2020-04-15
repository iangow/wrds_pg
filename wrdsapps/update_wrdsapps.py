#!/usr/bin/env python3
from wrds2pg import wrds_update

updated = wrds_update("firm_ratio", "wrdsapps", force=True)
