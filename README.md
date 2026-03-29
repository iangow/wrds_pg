Scripts to import WRDS data to PostgreSQL
=========

This repository contains scripts to import data from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) to a PostgreSQL database,
primarily using the `db2pq` Python package.

The scripts require that you have access to WRDS (and also to the data in question).
This repository uses a `uv`-based Python workflow, with `db2pq` configured as a local editable dependency via `pyproject.toml`.
## Rationale

Some natural questions you might ask are:

**Why have your own PostgreSQL database? Why not just use the WRDS PostgreSQL database?**

There are at least two reasons:

1. **Merging with non-WRDS data.**
There is no mechanism to get large amounts of data from outside WRDS onto the WRDS database (but see the `copy_inline` function in the [`dbplyr` package](https://dbplyr.tidyverse.org) for one approach).
Almost any project you will work on will include data from other sources.
Having your own database, along with a mechanism for easily getting WRDS data into it (as provided by the `db2pq` Python library) means you can just merge on your own hardware.
2. **Ability to store intermediate results.**
WRDS doesn't grant users even `CREATE TEMPORARY TABLE` privileges, so there is no mechanism for storing results of queries.
A common workflow involves queries that take minutes (or even hours) to run.
Having the ability to store the results of these queries somewhere is essential.
Being able to store them in a database is clearly superior to dumping them to alternative formats and then creating an import step.

**Why use WRDS PostgreSQL as the source instead of querying WRDS directly for every project?**

This repository now uses the WRDS PostgreSQL service as the source, with `db2pq` materializing selected tables into a local PostgreSQL database.

That workflow still has a few practical advantages:

1. **Local control over schema and indexes.**
You can add indexes, comments, derived tables, and other local structures that are specific to your own research workflow.
2. **Repeatable local updates.**
`db2pq` makes it straightforward to refresh a local mirror of selected WRDS tables and then work against that local copy.
3. **Integration with downstream workflows.**
The same tooling can support PostgreSQL-to-PostgreSQL and PostgreSQL-to-Parquet workflows, which is convenient when WRDS data is only one part of a larger pipeline.

Historically, earlier versions of this repository relied more heavily on WRDS SAS data.
That history explains some of the structure and some of the older design choices, but it is no longer the current workflow.

Finally, the `db2pq` package is *fast*.
It uses efficient PostgreSQL-based transfer paths and makes it practical to keep local mirrors of large WRDS tables up to date.
For example, merely downloading the 836 MB `vavotesresult.sas7bdat` file in the `risk` library takes 17 seconds. 

```bash
igow@Ians-Mac-mini-3 ~ % time scp "${WRDS_ID}"@wrds-cloud.wharton.upenn.edu:/wrds/riskmetrics/sasdata/voting_analytics/voteresults_us/vavoteresults.sas7bdat /tmp/

vavoteresults.sas7bdat                                                                                                                            100%  797MB  53.0MB/s   00:15    
scp  /tmp/  0.71s user 2.25s system 17% cpu 17.277 total
```

In contrast, `db2pq` takes a mere 6 seconds to populate my local PostgreSQL database!

```bash
uv run python
```

```python
>>> from db2pq import wrds_update_pg
>>> wrds_update_pg("vavoteresults", "risk", use_sas=True, force=True)
Forcing update based on user request.
Beginning file import at 2026-03-29 14:14:34 UTC.
Importing data into risk.vavoteresults.
Completed file import at 2026-03-29 14:14:40 UTC.

True
```

## Data sets covered

- [Audit Analytics](audit/readme.md) (`audit`)*
- [BoardEx](boardex/readme.md) (`boardex`)
- [Capital IQ](ciq/readme.md) (`ciq`)*
- [Compustat](comp/readme.md) (`comp`)
- [CRSP](crsp/readme.md) (`crsp`)*
- [CUSIP](cusipm/readme.md) (`cusipm`)
- [DealScan](dealscan/readme.md) (`dealscan`)*
- Fama-French (`ff`)*
- [IBES](ibes/readme.md) (`ibes`)*
- [KLD](kld/readme.md) (`kld`)*
- [MFLINKs](mflinks/readme.md) (`mflinks`)*
- [ISS](risk/readme.md) (`risk`)*
- [RavenPack](rpna/readme.md) (`rpna`)*
- [Thomson Reuters](tfn/readme.md) (`tfn`)*
- WRDS SEC Analytics (`wrdssec`)*

Most of the schemas above (those indicated by *) can be updated by running the script `upload_wrds.sh`.
Exceptions are:

- BoardEx (`boardex`): Because of the size of this data set and the frequency with which it is updated, updates for this one are only triggered by running `boardex/update_boardex.py`.
- Compustat (`comp`): Because of the size of this data set and the frequency with which it is updated (daily), updates for this one are only triggered by running `comp/update_comp.py`.
- ISS Voting Analytics (`risk`): The script `risk/update_risk.py` updates two data sets (`risk.voteanalysis_npx` and `risk.proposals`) to which I don't currently have access.


## Requirements

### Python (`uv`)

Install [`uv`](https://docs.astral.sh/uv/) if it is not already available, then set up the project environment from the repository root:

```bash
uv sync
```

This will install the Python dependencies declared in `pyproject.toml`, including the local editable checkout of `db2pq`.

Run scripts through `uv` so they use the project environment:

```bash
uv run python comp/update_comp.py
uv run python ciq/update_ciq.py
```

To open an interactive Python session with the same environment, use:

```bash
uv run python
```

### Local `.env`

`db2pq` automatically loads a local `.env` file via `python-dotenv` when resolving defaults. In this repository, the most useful variables are:

- `PGHOST`: destination PostgreSQL host
- `PGPORT`: destination PostgreSQL port
- `PGDATABASE`: destination PostgreSQL database
- `PGUSER`: destination PostgreSQL user
- `WRDS_ID`: WRDS username
- `DATA_DIR`: base directory for Parquet output, if you use the Parquet export helpers

Example `.env`:

```dotenv
PGHOST=localhost
PGPORT=5432
PGDATABASE=wrds
PGUSER=igow
WRDS_ID=your_wrds_id
DATA_DIR=/path/to/parquet
```

For WRDS PostgreSQL access, the preferred setup is to keep your password in `~/.pgpass` rather than in `.env`. The repository-level `.env` file is gitignored and is intended for local machine-specific settings.

### 1. R

A small number of scripts rely on R,
which can be obtained [here](https://cran.rstudio.com/).
We recommend [RStudio](https://www.rstudio.com/products/RStudio/);
in fact, this repository is set up as an RStudio project (open the file [wrds_pg.Rproj](https://github.com/iangow-public/wrds_pg/blob/master/wrds_pg.Rproj) in RStudio).

### 2. Bash

A small number of scripts here are Bash shell scripts.
These should work on Linux or MacOS, but may require modification to work on Windows.
