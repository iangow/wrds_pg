#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys

from wrds2pg import wrds2pg
from time import gmtime, strftime

updated = wrds2pg.wrds_update("wciklink_gvkey", "wrdssec")
updated = wrds2pg.wrds_update("wciklink_names", "wrdssec")
updated = wrds2pg.wrds_update("wciklink_cusip", "wrdssec", drop="tmatch")

