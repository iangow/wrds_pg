Code to import WRDS data to PostgreSQL
=========

This repository contains code to pull together data from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/).

The code will only work if you have access to WRDS and to the data in question.

The package used in this repository is available [here](https://github.com/iangow/wrds2pg). 

## Requirements

Instrcutions of Git, Python, WRDS, PostgreSQL and environment variables settings can be found [here](https://github.com/iangow/wrds2pg/blob/master/README.md).

### 1. R

A small number of scripts rely on R.
This can be obtained [here](https://cran.rstudio.com/).
I recommend [RStudio](https://www.rstudio.com/products/RStudio/);
in fact, this repository is set up as an RStudio project (open the file [wrds_pg.Rproj](https://github.com/iangow-public/wrds_pg/blob/master/wrds_pg.Rproj) in RStudio).

### 2. Bash

A small number of scripts here are Bash shell scripts.
These should work on Linux or OS X, but not on Windows (unless you have Cygwin or something like it; see [here](http://stackoverflow.com/questions/6413377/is-there-a-way-to-run-bash-scripts-on-windows)).

I may occasionally assume that `psql` (command-line interface to PostgreSQL) is on the path.
