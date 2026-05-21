#!/usr/bin/env python3
from db2pq import wrds_update_pg

wrds_update_pg("seg_customer", "compseg")
wrds_update_pg("names_seg", "compseg")
