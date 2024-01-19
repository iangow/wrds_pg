#!/usr/bin/env python3
from wrds2pg import wrds_update

# Auditor Changes
updated = wrds_update("feed55_auditor_ratification", "audit", 
                      drop="match: prior: closest: ",
                      col_types={"auditor_ratification_fkey": "integer",
                                 "share_class_fkey": "integer",
                                 "auditor_fkey": "integer",
                                 "pcaob_registration_number": "integer"})
