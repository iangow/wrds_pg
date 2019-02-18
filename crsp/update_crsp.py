#!/usr/bin/env python3
print("Updating CRSP", end="")
from sqlalchemy import create_engine
from wrds2pg import wrds_update, run_file_sql

print(".", end="")

print(".", end="")
import sys
sys.path.insert(0, '..')

from make_engine import engine, wrds_id
print(".")

# Update Treasury yield table crsp.tfz_ft
# From wrds:
# The error is correct, the table "tfz_ft," does not exist. Behind the scenes this web
# query form is joining two tables on the fly. The tables this query is joining are
# "crsp.tfz_idx" and either "crsp.tfz_dly_ft" or "crsp.tfz_mth_ft" (depending on if
# you want daily or monthly data) by the variable "kytreasnox."

# Here are some links to the information about these tables:
# https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77140
# https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77137
# https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77147
tfz_idx = wrds_update("tfz_idx", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
tfz_dly_ft = wrds_update("tfz_dly_ft", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if tfz_idx or tfz_dly_ft:
    sql = """
        DROP TABLE IF EXISTS crsp.tfz_ft;
        CREATE TABLE crsp.tfz_ft
        AS
        (SELECT kytreasnox, tidxfam, ttermtype, ttermlbl, caldt, rdtreasno,
                rdcrspid, -- to_timestamp(rdcrspid, 'YYYYMMDD.HH24MISS') as rdcrspid,
                tdyearstm, tdduratn, tdretadj, tdytm, tdbid, tdask, tdnomprc,
                tdnomprc_flg, tdaccint
        FROM crsp.tfz_idx
        INNER JOIN crsp.tfz_dly_ft
        USING (kytreasnox))
        """
    engine.execute(sql)
    engine.execute("ALTER TABLE crsp.tfz_ft ALTER kytreasnox TYPE integer")
    engine.execute("ALTER TABLE crsp.tfz_ft ALTER ttermtype TYPE integer")
    engine.execute("ALTER TABLE crsp.tfz_ft ALTER rdtreasno TYPE integer")
    engine.execute("ALTER TABLE crsp.tfz_ft OWNER TO crsp")
    engine.execute("GRANT SELECT ON crsp.tfz_ft TO crsp_access")

mse = wrds_update("mse", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)

# Update monthly data
msf = wrds_update("msf", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if msf:
    engine.execute("CREATE INDEX ON crsp.msf (permno, date)")

msi = wrds_update("msi", "crsp", engine=engine, wrds_id=wrds_id)

msedelist = wrds_update("msedelist", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)

mport = wrds_update("mport1", "crsp", engine=engine, wrds_id=wrds_id)
if mport:
    print("Getting ermport1")
    from wrds_fetch import get_process, wrds_process_to_pg

    sas_code = """
        proc sql;
            CREATE TABLE ermport AS
            SELECT date, capn AS capn, avg(decret) AS decret
            FROM crsp.ermport1
            GROUP BY date, capn
            ORDER BY date, capn;
        quit;

    	* Now dump it out to a CSV file;
    	proc export data=ermport outfile=stdout dbms=csv;
    	run;
	"""

    sql_ermport = """
        CREATE TABLE crsp.ermport
        (
            date date,
            capn bigint,
            decret double precision
        );
    """

    res = engine.execute("DROP TABLE IF EXISTS crsp.ermport CASCADE")
    res = engine.execute(sql_ermport)

    p = get_process(sas_code, wrds_id)
    res = wrds_process_to_pg("ermport", "crsp", engine, p)
    run_file_sql("crsp_make_ermport1.sql", engine)
    engine.execute("ALTER TABLE crsp.ermport OWNER TO crsp")
    engine.execute("GRANT SELECT ON crsp.ermport TO crsp_access")


if msi:
    engine.execute("CREATE INDEX ON crsp.msi (date)")

if mport or msf or msi or msedelist:
    run_file_sql("crsp_make_mrets.sql", engine)

# Update daily data
dsf = wrds_update("dsf", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if dsf:
    engine.execute("ALTER TABLE crsp.dsf ALTER permno TYPE integer")
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON crsp.dsf (permno, date)")

dsi = wrds_update("dsi", "crsp", engine=engine, wrds_id=wrds_id)
if dsi:
    engine.execute("CREATE INDEX ON crsp.dsi (date)")
    run_file_sql("make_trading_dates.sql", engine)

dsedelist = wrds_update("dsedelist", "crsp", engine=engine, wrds_id=wrds_id,
                            fix_missing=True)
if dsedelist:
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dsedelist (permno)")

dport = wrds_update("dport1", "crsp", engine=engine, wrds_id=wrds_id)
if dport:
    engine.execute("ALTER TABLE crsp.dport1 ALTER permno TYPE integer")

if dport:
    print("Getting ermport1")
    from wrds_fetch import get_process, wrds_process_to_pg

    sas_code = """
        proc sql;
            CREATE TABLE erdport AS
            SELECT date, capn AS capn, avg(decret) AS decret
            FROM crsp.erdport1
            GROUP BY date, capn
            ORDER BY date, capn;
        quit;

    	* Now dump it out to a CSV file;
    	proc export data=erdport outfile=stdout dbms=csv;
    	run;
	"""

    sql_erdport = """
        CREATE TABLE crsp.erdport
        (
            date date,
            capn bigint,
            decret double precision
        );
    """

    res = engine.execute("DROP TABLE IF EXISTS crsp.erdport CASCADE")
    res = engine.execute(sql_erdport)

    p = get_process(sas_code, wrds_id)
    res = wrds_process_to_pg("erdport", "crsp", engine, p)

    run_file_sql("crsp_make_erdport1.sql", engine)
    engine.execute("CREATE INDEX ON crsp.dport1 (permno, date)")
    engine.execute("ALTER TABLE crsp.erdport OWNER TO crsp")
    engine.execute("GRANT SELECT ON TABLE crsp.erdport TO crsp_access")

if dport or dsf or dsi or dsedelist:
    run_file_sql("crsp_make_rets_alt.sql", engine)

ccmxpf_linktable = wrds_update("ccmxpf_linktable", "crsp", engine=engine, wrds_id=wrds_id,
                                fix_missing=True)
if ccmxpf_linktable:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermno TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermco TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER usedflag TYPE integer")

ccmxpf_lnkhist = wrds_update("ccmxpf_lnkhist", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if ccmxpf_lnkhist:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)")

dsedist = wrds_update("dsedist", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if dsedist:
    engine.execute("CREATE INDEX ON crsp.dsedist (permno)")
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer")

stocknames = wrds_update("stocknames", "crsp", engine=engine, wrds_id=wrds_id)
if stocknames:
    engine.execute("ALTER TABLE crsp.stocknames ALTER permno TYPE integer")
    engine.execute("ALTER TABLE crsp.stocknames ALTER permco TYPE integer")

dseexchdates = wrds_update("dseexchdates", "crsp", engine=engine, wrds_id=wrds_id)
if dseexchdates:
    engine.execute("ALTER TABLE crsp.dseexchdates ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dseexchdates (permno)")

# Update other data sets
wrds_update("msp500list", "crsp", engine=engine, wrds_id=wrds_id)
wrds_update("ccmxpf_lnkused", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
# wrds_update("fund_names", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)

wrds_update("msp500", "crsp", engine=engine, wrds_id=wrds_id)
wrds_update("msp500p", "crsp", engine=engine, wrds_id=wrds_id)
wrds_update("mcti", "crsp", engine=engine, wrds_id=wrds_id)
wrds_update("msedist", "crsp", engine=engine, wrds_id=wrds_id)
wrds_update("mseshares", "crsp", engine=engine, wrds_id=wrds_id)

# Fix permissions.
engine.execute("GRANT USAGE ON SCHEMA crsp TO wrds_access")
engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA crsp TO wrds_access")

engine.dispose()
