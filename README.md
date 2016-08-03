Code to import WRDS data to PostgreSQL
=========

This repository contains code to pull together data from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/).

The code will only work if you have access to WRDS and to the data in question.

## Requirements

### 1. Git

While not strictly necessary to use the scripts here, [Git](https://git-scm.com/downloads) likely makes it easier to download and to update.

I keep all Git repositories in `~/git`. So to get this repository, I could do:

```
cd ~/git
git clone https://github.com/iangow/wrds_pg.git
```

This will create a copy of the repository in `~/git/wrds_pg`.
Note that one can get updates to the repository by going to the directory and "pulling" the latest code:

```
cd ~/git/wrds_pg
git pull
```

Alternatively, I think you could fork the repository on GitHub and then clone. 
I think that cloning using the SSH URL (e.g., `git@github.com:iangow/wrds_pg.git`) is necessary for Git pulling and pushing to work well in RStudio.

### 2. Perl

Many of the scripts rely on Perl (I use MacPorts, which I think currently defaults to v5.16).
In addition, the Perl scripts generally interact with PostgreSQL using the Perl
module `DBD::Pg` (see [here](http://search.cpan.org/dist/DBD-Pg/Pg.pm)). 
I use MacPorts to install this `sudo port install p5-dbd-pg`.
On Ubuntu, `sudo apt-get install libdbi-perl libdbd-pg-perl` would work.

### 3. R

A number of scripts rely on R.
This can be obtained [here](https://cran.rstudio.com/).
I recommend [RStudio](https://www.rstudio.com/products/RStudio/);
in fact, this repository is set up as an RStudio project (open the file [wrds_pg.Rproj](wrds_pg.Rproj) in RStudio).

### 4. PostgreSQL

You should have a PostgreSQL database to store the data.
There are also some data dependencies in that some scripts assume the existence of other data in the database.
Also, I assume the existence of a role `wrds` (SQL `CREATE ROLE wrds` works to add this if it is absent).

### 5. Bash

A number of scripts here are Bash shell scripts.
These should work on Linux or OS X, but not on Windows (unless you have Cygwin or something like it; see [here](http://stackoverflow.com/questions/6413377/is-there-a-way-to-run-bash-scripts-on-windows)).

I also assume that `psql` (command-line interface to PostgreSQL) is on the path.
I have MacPorts on my path (in `~/.profile` I set `export PATH=/opt/local/bin:/opt/local/sbin:$PATH`) and I can ensure that PostgreSQL is on my path by setting `sudo port select postgresql postgresql94` (v9.4 being current at the time of writing).

### 6. Environment variables

I am migrating the scripts, etc., from using hard-coded values (e.g., my WRDS ID `iangow`) to using environment variales. 
Environment variables that I use include:

- `PGDATABASE`: The name of the PostgreSQL database you use.
- `PGUSER`: Your username on the PostgreSQL database.
- `PGHOST`: Where the PostgreSQL database is to be found (this will be `localhost` if its on the same machine as you're running the code on)
- `WRDS_ID`: Your [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) ID.
- `PGBACKUP_DIR`: The directory where backups of PostgreSQL data created by `pg_dump` should go.

I set these environment variables in `~/.profile`:

```
export PGHOST="localhost"
export PGDATABASE="crsp"
export WRDS_ID="iangow"
export PGUSER="igow"
export PGBACKUP_DIR="/Users/igow/Dropbox/pg_backup/"
```

I also set them in `~/.Rprofile`, as RStudio doesn't seem to pick up the settings in `~/.profile` in recent versions of OS X:

```
Sys.setenv(PGHOST="localhost")
Sys.setenv(PGDATABASE="crsp")
Sys.setenv(PGBACKUP_DIR="/Users/igow/Dropbox/pg_backup/")
```

### 7. A WRDS ID

Note that I use public-key authentication to access WRDS. Following hints taken from [here](http://www.debian-administration.org/articles/152), I set up a public key. I then copied that key to the WRDS server from the terminal on my computer. (Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) Here's code to create the key and send it to WRDS (for me):

```
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh iangow@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```

I use an empty passphrase in setting up my key so that the scripts can run without user intervention.

## Illustration of use of scripts

- `wrds_fetch.pl`: This Perl script takes the following arguments:
    - `--fix-missing`: SAS's `PROC EXPORT` converts special missing values (e.g., `.B`) to strings. So my code converts these to "regular" missing values so that PostgreSQL can handle them as missing values of the correct type.
    - `--wrds-id=wrds_id`: Specify your WRDS ID here. My WRDS ID is `iangow`, so I say `--wrds-id=iangow` here. It's probably preferable to set this using an environment variable, especially if using scripts that do not accept a `--wrds-id` argument.
    - `--dbname=dbname`: My database is called `crsp`, so I would say `--dbname=crsp` here.  It's probably preferable to set this using an environment variable, especially if using scripts that do not accept a `--wrds-id` argument.
    - `--updated=some_string`: This is used by the script `wrds_update` to check if the table on WRDS has been updated since it was last pulled into the database.
    - `--obs=obs`: Optional argument to limit the number of observations imported from WRDS. For example, `--obs=1000` will limit the data to 1000 observations.

- `wrds_update.pl`: Except for `updated` this has all the options that `wrds_to_pg.pl` has. But this script compares the local and WRDS versions of the data and only updates if it needs to do so. Additionally, `wrds_update.pl` accepts a command-line argument `--force`, which forces update regardless of whether an update has occurred (this is useful for debugging).

So 
```
wrds_update.pl crsp.msi --wrds_id=iangow --dbname=crsp
```
updates the monthly stock index file from CRSP and 
```
wrds_update.pl crsp.msf --wrds_id=iangow --dbname=crsp --fix-missing
```
updates the monthly stock file from CRSP (this file has special missing values, hence the additional flag).
