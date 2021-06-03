#!/usr/bin/env python3
from wrds2pg import wrds_update

wrds_update("seg_customer", "compsegd")
wrds_update("names_seg", "compsegd")
