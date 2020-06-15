Scripts to import WRDS data to PostgreSQL
=========

This repository contains scripts to import data from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) to a PostgreSQL database,
primarily using the `wrds2pg` Python package.

The scripts require that you install `wrds2pg` and have access to WRDS (and also to the data in question). 
The `wrds2pg` package is available for installation via `pip` (see [here](https://pypi.org/project/wrds2pg)). 

## Data sets covered

- [Audit Analytics](audit/readme.md) (`audit`)
- [BoardEx]
- [Capital IQ]
- [Compustat]
- [CRSP]
- [CUSIP]
- [DealScan]
- [IBES]
- [KLD]
- [MFLINKs]
- [ISS]
- [RavenPack]
- [TFN]

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
