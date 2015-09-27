#!/usr/bin/env bash
psql -d crsp < remove_dependencies.sql
pg_restore --clean --host localhost --username "igow" --dbname "crsp"   --verbose --no-tablespaces --jobs=4  ~/Dropbox/pg_backup/crsp.backup 
psql -d crsp < ~/Dropbox/research/activism/targeted/code/create_activism_permnos.sql 
psql -d crsp < ~/Dropbox/research/activism/targeted/code/create_targeted.sharkwatch.sql 
R CMD BATCH ~/Dropbox/research/activism/activist_director/code/create_sharkwatch_sample.R 
psql -d crsp < ~/Dropbox/research/activism/activist_director/code/create_director_bio_matched.sql 
