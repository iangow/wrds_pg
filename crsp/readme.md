# CRSP Scripts

This directory contains scripts for building and maintaining the local `crsp` PostgreSQL schema from WRDS data.

## Main updater

- `update_crsp.py`: Main import/update script.
  - Pulls core CRSP tables from WRDS into local Postgres using `wrds_update`.
  - Applies type fixes (for example, `permno`/`permco` as integers).
  - Creates key indexes after refreshes.
  - Rebuilds `crsp.tfz_ft` (joined from `tfz_idx` + `tfz_dly_ft`) to mirror WRDS web-query behavior.

## SQL helper scripts

- `eomonth.sql`: Defines `eomonth()` SQL functions used by monthly return logic.
  - This is still part of the active workflow if queries/functions depend on `eomonth()`.

## Legacy SQL scripts (usually not run)

- `crsp_make_rets.sql`: Builds `crsp.rets` (daily returns combined with delisting returns), then indexes and grants.
- `crsp_make_mrets.sql`: Builds `crsp.mrets` (monthly returns combined with delisting returns), then indexes and grants.
- `make_trading_dates.sql`: Builds date-mapping tables used in event studies:
  - `crsp.trading_dates` (trading date -> trading-day index `td`)
  - `crsp.anncdates` (calendar announcement date -> next trading date / `td`)

These derived-table scripts are not part of the current regular workflow. Replacement approaches are documented in:
https://iangow.github.io/far_book/beaver68.html

## Typical usage

1. Run `update_crsp.py` to refresh core CRSP tables.
2. Run `eomonth.sql` if needed.

This is a practical workflow directory: the scripts are designed for recurring local data maintenance rather than as a standalone package.
