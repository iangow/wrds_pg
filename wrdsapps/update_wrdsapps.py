#!/usr/bin/env python3
from wrds2pg import wrds_update

# updated = wrds_update("firm_ratio", "wrdsapps")
updated = wrds_update("bondret", "wrdsapps",
                       col_types= {'t_volume': 'text',
                                   't_dvolume': 'text',
                                   't_spread': 'text',
                                   'yield': 'text',
                                   'ret_eom': 'text',
                                   'ret_l5m': 'text',
                                   'ret_ldm': 'text'})
wrds_update("ibcrsphist", "wrdsapps")
