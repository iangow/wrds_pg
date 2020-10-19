Scripts to import WRDS data to PostgreSQL
=========

This repository contains scripts to import data from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) to a PostgreSQL database,
primarily using the `wrds2pg` Python package.

The scripts require that you install `wrds2pg` and have access to WRDS (and also to the data in question). 
The `wrds2pg` package is available for installation via `pip` (see [here](https://pypi.org/project/wrds2pg)). 

## Rationale

Some natural questions you might ask are:

**Why have your own PostgreSQL database? Why not just use the WRDS PostgreSQL database?**

There are at least two reasons:

1. **Merging with non-WRDS data.** 
There is no mechanism to get data from outside WRDS onto the WRDS database. 
Almost any project you will work on will include data from other sources. 
Having your own database, along with a mechanism for easily getting WRDS data into it (as provided by the `wrds2pg` Python library) means you can just merge on your own hardware.
2. **Ability to store intermediate results.**
WRDS doesn't grant users even `CREATE TEMPORARY TABLE` privileges, so there is no mechanism for storing results of queries.
A common workflow involves queries that take minutes (or even hours) to run.
Having the ability to store the results of these queries somewhere is essential.
Being able to store them in a database is clearly superior to dumping them to alternative formats and then creating an import step.

**Why use WRDS's SAS data, not the PostgreSQL database, as the source?**

One answer is that the the `wrds2pg` package effectively started in around 2011, which is long before WRDS had a PostgreSQL database.

Another answer is that, while WRDS has a PostgreSQL database, it seems clear that SAS is their primary focus.
SAS basically has two data types: floating-point numeric and fixed-width characters.
The WRDS PostgreSQL database often seems to inherit data types from WRDS SAS files.
So fields that should be `text` are instead `varchar`, even though the latter format just [adds overhead](https://stackoverflow.com/questions/4848964/postgresql-difference-between-text-and-varchar-character-varying).
Also, fields that are integers are stored as `double precision` in the PostgreSQL database (e.g., `permno` on CRSP data sets).

Another sign of the SAS data provenance of WRDS's PostgreSQL data is the retention of variables that only make sense on SAS, such as string and numerical representations of dates.

In contrast, the `wrds2pg` Python library examines the SAS data sets and attempts to infer the appropriate data type for each field.
So integers are stored as `integer` and dates are stored as `date`.
Inferring type relies on formatting in the SAS data files, and WRDS is very inconsistent in how carefully data sets are formatted.
As a result, some times it's necessary to manually specify data types, which are inferred by visual inspection of the data or background information (e.g., we know that PERMNOs are integers).

Another advantage of using the SAS data files is that they include information about when they were last modified. 
The `wrds2pg` package uses this information to do updates if and only if necessary.
This makes maintaining a local subset of WRDS much easier.

Finally, the `wrds2pg` package is *fast*. 
I don't think one could match the performance of the `wrds2pg` package, which uses data compression and PostgreSQL's `COPY` function to import data.
For example, merely downloading the 530MB `vavotesresult.sas7bdat` file in the `risk` library takes 63 seconds.
In contrast, `wrds2pg` takes **9 seconds**!

```python
igow@igow-ubuntu-mate:~/git/wrds_pg$ python3
Python 3.8.2 (default, Apr 27 2020, 15:53:34) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from wrds2pg import wrds_update
>>> wrds_update("vavoteresults", "risk")
Updated risk.vavoteresults is available.
Getting from WRDS.

Beginning file import at 14:58:20.
Importing data into risk.vavoteresults
Completed file import at 14:58:29.
True
>>>
```

## Data sets covered

- [Audit Analytics](audit/readme.md) (`audit`)\*
- [BoardEx](boardex/readme.md) (`boardex`)
- [Capital IQ](ciq/readme.md) (`ciq`)\*
- [Compustat](comp/readme.md) (`comp`)
- [CRSP](crsp/readme.md) (`crsp`)\*
- [CUSIP](cusipm/readme.md) (`cusipm`)
- [DealScan](dealscan/readme.md) (`dealscan`)\*
- Fama-French (`ff`)\*
- [IBES](ibes/readme.md) (`ibes`)\*
- [KLD](kld/readme.md) (`kld`)\*
- [MFLINKs](mflinks/readme.md) (`mflinks`)\*
- [ISS](risk/readme.md) (`risk`)\*
- [RavenPack](rpna/readme.md) (`rpna`)\*
- [Thomson Reuters](tfn/readme.md) (`tfn`)\*
- WRDS SEC Analytics (`wrdssec`)\*

Most of the schemas above (those indicated by \*) can be updated by running the script `upload_wrds.sh`.
Exceptions are:

- BoardEx (`boardex`): Because of the size of this data set and the frequency with which it is updated, updates for this one are only triggered by running `boardex/update_boardex.py`.
- Compustat (`comp`): Because of the size of this data set and the frequency with which it is updated (daily), updates for this one are only triggered by running `comp/update_comp.py`.
- ISS Voting Analytics (`risk`): The script `risk/update_risk.py` updates two data sets (`risk.voteanalysis_npx` and `risk.proposals`) to which I don't currently have access.


## Requirements

Step-by-step instructions to install and use `wrds2pg` package can be found [here](https://github.com/iangow/wrds2pg/blob/master/README.md).

### 1. R

A small number of scripts rely on R,
which can be obtained [here](https://cran.rstudio.com/).
We recommend [RStudio](https://www.rstudio.com/products/RStudio/);
in fact, this repository is set up as an RStudio project (open the file [wrds_pg.Rproj](https://github.com/iangow-public/wrds_pg/blob/master/wrds_pg.Rproj) in RStudio).

### 2. Bash

A small number of scripts here are Bash shell scripts.
These should work on Linux or MacOS, but may require modification to work on Windows.
