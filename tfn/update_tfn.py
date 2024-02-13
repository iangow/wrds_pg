#!/usr/bin/env python3
from wrds2pg import wrds_update

updated = wrds_update("amend", "tfn")
updated = wrds_update("avgreturns", "tfn", 
                      col_types = {"buycount": "float8"})
updated = wrds_update("company", "tfn", fix_cr=True)
updated = wrds_update("form144", "tfn", sas_encoding="wlatin1")
updated = wrds_update("header", "tfn", sas_encoding="wlatin1")
updated = wrds_update("idfhist", "tfn", sas_encoding="wlatin1")
updated = wrds_update("idfnames", "tfn")
updated = wrds_update("rule10b5", "tfn")
updated = wrds_update("table1", "tfn", sas_encoding="wlatin1")
updated = wrds_update("table2", "tfn", sas_encoding="wlatin1")
updated = wrds_update("s12type1", "tfn")
updated = wrds_update("s12type2", "tfn")
updated = wrds_update("s34", "tfn")
updated = wrds_update("s34type1", "tfn")
updated = wrds_update("s34type2", "tfn")
