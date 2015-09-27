#!/usr/bin/env bash
./wrds_update.pl gmi.takeoverdefenses
./wrds_update.pl gmi.takeoverdefenses2005
./wrds_update.pl gmi.takeoverdefenses2006
./wrds_update.pl gmi.takeoverdefenses2007
./wrds_update.pl gmi.takeoverdefenses2008
# ./wrds_update.pl gmi.takeoverdefenses2009
./wrds_update.pl gmi.takeoverdefenses2010
./wrds_update.pl gmi.takeoverdefenses2011
./wrds_update.pl gmi.takeoverdefenses2012
./wrds_update.pl gmi.takeoverdefenses2013
./wrds_update.pl gmi.names

# The 2008 table is missing the "year" field
psql -c "ALTER TABLE gmi.takeoverdefenses2008 ADD COLUMN year integer;"
psql -c "UPDATE gmi.takeoverdefenses2008 SET year=2008;"

psql -c "ALTER TABLE gmi.takeoverdefenses ALTER COLUMN year TYPE integer"
psql -c "ALTER TABLE gmi.takeoverdefenses2009 ALTER COLUMN year TYPE integer"
psql -c "ALTER TABLE gmi.takeoverdefenses2010 ALTER COLUMN year TYPE integer"
psql -c "ALTER TABLE gmi.takeoverdefenses2011 ALTER COLUMN year TYPE integer"
psql -c "ALTER TABLE gmi.takeoverdefenses2012 ALTER COLUMN year TYPE integer"
psql -c "ALTER TABLE gmi.takeoverdefenses2013 ALTER COLUMN year TYPE integer"

Rscript "gmi/get_tod_2009.R"
Rscript "gmi/combine_tables.R"
