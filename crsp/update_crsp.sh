#!/usr/bin/env bash
psql -f code/eomonth.sql
perl crsp/update_crsp.pl
