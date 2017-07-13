Code to import WRDS data to PostgreSQL
=========

This repository contains code to pull together data from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/).

The code will only work if you have access to WRDS and to the data in question.

PGUSER should be superuser.

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

### 2. Python

Most of the scripts rely on Python (I use Python 3).
In addition, the Python scripts generally interact with PostgreSQL using the `psycopg` (see [here](http://initd.org/psycopg/)) and SQLAlchemy.
Additionally, the scripts use Pandas and Paramiko.

### 3. A WRDS ID

Note that I use public-key authentication to access WRDS. Following hints taken from [here](http://www.debian-administration.org/articles/152), I set up a public key. I then copied that key to the WRDS server from the terminal on my computer. (Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) Here's code to create the key and send it to WRDS (for me):

```
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh iangow@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```

I use an empty passphrase in setting up my key so that the scripts can run without user intervention.

### 4. PostgreSQL

You should have a PostgreSQL database to store the data.
There are also some data dependencies in that some scripts assume the existence of other data in the database.
Also, I assume the existence of a role `wrds` (SQL `CREATE ROLE wrds` works to add this if it is absent).


### 5. Environment variables

I am migrating the scripts, etc., from using hard-coded values (e.g., my WRDS ID `iangow`) to using environment variales. 
Environment variables that I use include:

- `PGDATABASE`: The name of the PostgreSQL database you use.
- `PGUSER`: Your username on the PostgreSQL database.
- `PGHOST`: Where the PostgreSQL database is to be found (this will be `localhost` if its on the same machine as you're running the code on)
- `WRDS_ID`: Your [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) ID.

I set these environment variables in `~/.profile`:

```
export PGHOST="localhost"
export PGDATABASE="crsp"
export WRDS_ID="iangow"
export PGUSER="igow"
```

```
source ~/.profile
```

I also set them in `~/.Rprofile`, as RStudio doesn't seem to pick up the settings in `~/.profile` in recent versions of OS X:

```
Sys.setenv(PGHOST="localhost")
Sys.setenv(PGDATABASE="crsp")
```

### 6. R

A small number of scripts rely on R.
This can be obtained [here](https://cran.rstudio.com/).
I recommend [RStudio](https://www.rstudio.com/products/RStudio/);
in fact, this repository is set up as an RStudio project (open the file [wrds_pg.Rproj](wrds_pg.Rproj) in RStudio).

### 7. Bash

A small number of scripts here are Bash shell scripts.
These should work on Linux or OS X, but not on Windows (unless you have Cygwin or something like it; see [here](http://stackoverflow.com/questions/6413377/is-there-a-way-to-run-bash-scripts-on-windows)).

I may occasionally assume that `psql` (command-line interface to PostgreSQL) is on the path.
