## WRDS to PG Migration
Function:
- Downloads tables from wrds and uploads to PG.
- Upload existing table in SAS format to PG.

#### 1. PG Settings
Users are expected to specify `PGHOST (-h`), `PGPORT (-p`), `PGDATABASE (-d`), `schema (-s`), `table (-t`), `PGUSER (-u`).

`PGHOST`, `PGDATABASE`, `PGUSER` will be grepped from OS environment variables if not specified by user. Default `PGPORT` is`5432`.

#### 2. WRDS Settings
`WRDS_ID` can be read from command line via `-w`, otherwise the software will grep from OS environment variables.

#### 3. Table specific settings
For table specific settings, 

`--fix_missing`: set to `True` to fix missing values. Default value is `False`. eg. `--fox_missing True`.

`--fix_cr`: set to `True` to fix characters. Default value is `False`.

`--drop`: add column names to be dropped.eg.`--drop "id name"` will drop column `id` and `name`.

`--obs`: add maxium number of observations. eg.`--obs 10` will export the top 10 rows from the table.

`--rename`: rename columns. eg.`--rename "fee=mngt_fee"` rename `fee` to `mngt_fee`.

`--force`: set to `True` to force update. Default value is `False`.

#### 4. Upload SAS File
To upload existing SAS file to PG, you need to have local SAS installed.

Use `-P` to specify file path.

#### 5. Use this program
Here is an example.

`./wrds_update -h host -d pgdatabase -u pguser --fix_missing True --drop "id name"`

#### 6. Help
To get help, use `./wrds_update --help`.
```bash
usage: ./wrds_update [--help] [-h [HOST]] [-p [PORT]] [-d [DBNAME]]
                     [-f [FPATH]] -t [TABLE] -s [SCHEMA] [-w [WRDS_ID]]
                     [-u [PGUSER]] [--fix_missing] [--fix_cr] [--drop] [--obs]
                     [--rename] [--force]

Functions
--------------------------------
1. Download tables from WRDS and upload to PostgreSQL.
2. Upload sas file to PostgreSQL.

Optional arguments:
  --help          Show this help message and exit

Connection arguments:
  -h [HOST]       PostgreSQL host
  -p [PORT]       PostgreSQL port
  -d [DBNAME]     PostgreSQL database
  -f [FPATH]      File path
  -t [TABLE]      Table name
  -s [SCHEMA]     WRDS library name
  -w [WRDS_ID]    WRDS ID
  -u [PGUSER]     PostgreSQL user

Table arguments:
  --fix_missing   Fix special missing values
  --fix_cr        Fix character
  --drop          Columns to drop
  --obs           Number of observations to return
  --rename        Rename columns
  --force         Force update

Report bugs to <ian.gow@unimelb.edu.au>.
```
