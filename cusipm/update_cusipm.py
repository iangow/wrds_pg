#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
from wrds2pg import wrds2pg

updated = wrds2pg.wrds_update("issuer", "cusipm")
updated = wrds2pg.wrds_update("issue", "cusipm")
