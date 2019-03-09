#!/usr/bin/env python3
import sys
sys.path.insert(0, '..')

from wrds_fetch import wrds_update
from make_engine import engine, wrds_id

updated = wrds_update("issuer", "cusipm", engine, wrds_id)
updated = wrds_update("issue", "cusipm", engine, wrds_id)
