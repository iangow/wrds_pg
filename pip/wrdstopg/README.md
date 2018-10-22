## WRDS to PG Migration
This software has two functions:
- Download tables from wrds and uploads to PG. 
- Upload sas file (`*.sas7dbat`) to PG.

#### 1. Python
The software uses Python 3 and depends on Pandas, SQLAlchemy and Paramiko.

#### 2. WRDS Settings
Set `WRDS_ID` with `wrds_id=your_wrds_id`, otherwise the software will grep from OS environment variables.

To use public-key authentication to access WRDS, follow hints [here](https://debian-administration.org/article/152/Password-less_logins_with_OpenSSH). First set up a public key, then copied that key to the WRDS server from terminal. 

(Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) 

Here's code to create the key and send it to WRDS.

```
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh your_wrds_id@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```
Use an empty passphrase in setting up my key so that the scripts can run without user intervention.

#### 3. PG Settings
If you have set `PGHOST`, `PGDATABASE`, `PGUSER` as environment variables, the software can grep them. Otherwise, users are expected to specify them when using `wrds_udpate()`. Default `PGPORT` is`5432`.

Two variables `table` and `schema` are required.

#### 4. Table Settings
To tailor tables, specify the following variables:

`fix_missing`: set to `True` to fix missing values. Default value is `False`. 

`fix_cr`: set to `True` to fix characters. Default value is `False`.

`drop`: add column names to be dropped.eg.`drop="id name"` will drop column `id` and `name`.

`obs`: add maxium number of observations. eg.`obs=10` will export the top 10 rows from the table.

`rename`: rename columns. eg.`rename="fee=mngt_fee"` rename `fee` to `mngt_fee`.

`force`: set to `True` to force update. Default value is `False`.

#### 5. Upload SAS File
The software can also upload SAS file directly to PG. You need to have local SAS in order to use this function.

Use `fpath` to specify file path.

#### 6. Examples
Here are some examples.

```py
from wrdstopg import wrdstopg

# 1. Download crsp.mcti from wrds and upload to pg as crps.mcti
# Simplest version
wrdstopg.wrds_update(table="mcti", schema="crsp")
# Tailor table to your needs
wrdstopg.wrds_update(table="mcti", schema="crsp", host=your_pghost, dbname=your_pg_database, fix_missing=True, 
	fix_cr=True, drop="b30ret b30ind", obs=10, rename="caldt=calendar_date", force=True)

# 2. Upload test.sas7dbat to pg as crsp.mcti
wrdstopg.wrds_update(table="mcti", schema="crsp", fpath="your_path/test.sas7dbat")
```

#### 7. Report Bugs
Author: Ian Gow, <ian.gow@unimelb.edu.au>

Contributor: Jingyu Zhang, <jingyu.zhang@chicagobooth.edu>