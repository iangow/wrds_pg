#!/usr/bin/env bash
psql -f crsp/eomonth.sql
perl crsp/update_crsp.pl
