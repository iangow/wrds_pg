#!/usr/bin/env python3
from wrds2pg.wrds2pg import wrds_update

updated = wrds_update("history", "kld", fix_missing=True)
updated = wrds_update("kldnames", "kld", fix_missing=True)
