#!/usr/bin/env python3
from db2pq import wrds_update_pg

updated = wrds_update_pg("amend", "tfn")
updated = wrds_update_pg("avgreturns", "tfn", 
                      col_types = {"buycount": "float8"})
updated = wrds_update_pg("company", "tfn",)
updated = wrds_update_pg("form144", "tfn")
updated = wrds_update_pg("header", "tfn")
updated = wrds_update_pg("idfhist", "tfn")
updated = wrds_update_pg("idfnames", "tfn")
updated = wrds_update_pg("rule10b5", "tfn")
updated = wrds_update_pg("table1", "tfn")
updated = wrds_update_pg("table2", "tfn")
updated = wrds_update_pg("s12type1", "tfn")
updated = wrds_update_pg("s12type2", "tfn")
updated = wrds_update_pg("s34", "tfn")
updated = wrds_update_pg("s34type1", "tfn")
updated = wrds_update_pg("s34type2", "tfn")
