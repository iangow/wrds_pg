# wrds_pg

This repository is a practical collection of WRDS data import/update scripts accumulated over the years.

Most scripts load specific WRDS libraries/tables into a local PostgreSQL database, historically via `wrds2pg` and, increasingly, via `db2pq`.

Structure is mostly by schema/source (for example: `audit/`, `comp/`, `crsp/`, `ibes/`, `risk/`), with each directory containing one or more update scripts and, in many cases, a local `readme.md`.

There is no single unified framework here; this is a working script repository that has grown incrementally as data needs changed.
