## WRDS to PG Migration
This software downloads tables from wrds and uploads to PG.

#### 1. PG Settings
Users are expected to specify `PGHOST (-H`), `PGPORT (-P`), `PGDATABASE (-D`), `schema (-S`), `table (-T`), `PGUSER (-U`).

`PGHOST`, `PGDATABASE`, `PGUSER` will be grepped from OS environment variables if not specified by user. Default `PGPORT` is`5432`.

#### 2. WRDS Settings
`WRDS_ID` can be read from command line via `-W`, otherwise the software will grep from OS environment variables.

#### 3. Table specific settings
For table specific settings, 

`--fix_missing`: set to `True` to fix missing values.Default value is `False`. eg. `--fox_missing True`.

`--fix_cr`: set to `True` to fix characters. Default value is `False`.

`--drop`: add column names to be dropped.eg.`--drop "id name"` will drop column `id` and `name`.

`--obs`: add maxium number of observations. eg.`--obs 10` will export the top 10 rows from the table.

`--rename`: rename columns. eg.`--rename "fee=mngt_fee"` rename `fee` to `mngt_fee`.

`--force`: set to `True` to force update. Default value is `False`.

#### 4. Upload SAS File
The software can also upload SAS file directly to PG. You need to have local SAS in order to use this function.

Use `-P` to specify file path.

#### 5. Use this program
Here is an example.

`./wrds_update -H host -D pgdatabase -U pguser --fix_missing True --drop "id name"`

#### 6. Help
To get help, use `./wrds_update --help`.
```bash
usage: wrds_update [-h] [-H [HOST]] [-P [PORT]] [-D [DBNAME]] [-F [FPATH]]
                   [-T [TABLE]] [-S [SCHEMA]] [-W [WRDS_ID]] [-U [PGUSER]]
                   [--fix_missing [FIX_MISSING]] [--fix_cr [FIX_CR]]
                   [--drop [DROP]] [--obs [OBS]] [--rename [RENAME]]
                   [--force [FORCE]]

Download tables from WRDS and upload to PostgreSQL.

optional arguments:
  -h, --help            show this help message and exit
  -H [HOST]             pghost address
  -P [PORT]             pgport
  -D [DBNAME]           pgdatabase
  -F [FPATH]            file path
  -T [TABLE]            table name
  -S [SCHEMA]           schema name
  -W [WRDS_ID]          wrds id
  -U [PGUSER]           pguser
  --fix_missing [FIX_MISSING]
                        fix missing value
  --fix_cr [FIX_CR]     fix character
  --drop [DROP]         columns to drop
  --obs [OBS]           number of observations to return
  --rename [RENAME]     rename columns
  --force [FORCE]       force update
```
