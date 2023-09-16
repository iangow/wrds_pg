#!/usr/bin/env python3
from wrds2pg import wrds_update

wrds_update("seg_customer", "comp_segments_hist_daily",
            sas_schema="compsegd")
wrds_update("names_seg", "comp_segments_hist_daily",
            sas_schema="compsegd")
