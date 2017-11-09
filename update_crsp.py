#!/usr/bin/env python3
print("Updating CRSP", end="")
from sqlalchemy import create_engine
print(".", end="")
import os
dbname = os.getenv("PGDATABASE")
print(".", end="")
host = os.getenv("PGHOST", "localhost")
print(".", end="")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

print(".", end="")
from wrds_fetch import wrds_update, run_file_sql

print(".")

msf = wrds_update("msf", "crsp", engine, wrds_id, fix_missing=True)
if msf:
    engine.execute("CREATE INDEX ON crsp.msf (permno, date)")

msi = wrds_update("msi", "crsp", engine, wrds_id)

msedelist = wrds_update("msedelist", "crsp", engine, wrds_id, fix_missing=True)

mport = wrds_update("mport1", "crsp", engine, wrds_id)
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
    run_file_sql("crsp/crsp_make_ermport1.sql", engine)

if msi:
    engine.execute("CREATE INDEX ON crsp.msi (date)")

if mport or msf or msi or msedelist:
    run_file_sql("crsp/crsp_make_mrets.sql", engine)

# Update daily data
dsf = wrds_update("dsf", "crsp", engine, wrds_id, fix_missing=True)
if dsf:
    engine.execute("ALTER TABLE crsp.dsf ALTER permno TYPE integer")
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON crsp.dsf (permno, date)")

dsi = wrds_update("dsi", "crsp", engine, wrds_id)
if dsi:
    engine.execute("CREATE INDEX ON crsp.dsi (date)")
    run_file_sql("crsp/make_trading_dates.sql", engine)

dsedelist = wrds_update("dsedelist", "crsp", engine, wrds_id,
                            fix_missing=True)
if dsedelist:
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dsedelist (permno)")

dport = wrds_update("dport1", "crsp", engine, wrds_id)
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

    run_file_sql("crsp/crsp_make_erdport1.sql", engine)
    engine.execute("CREATE INDEX ON crsp.dport1 (permno, date)")

if dport or dsf or dsi or dsedelist:
    run_file_sql("crsp/crsp_make_rets_alt.sql", engine)

ccmxpf_linktable = wrds_update("ccmxpf_linktable", "crsp", engine, wrds_id,
                                fix_missing=True)
if ccmxpf_linktable:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermno TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermco TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER usedflag TYPE integer")

ccmxpf_lnkhist = wrds_update("ccmxpf_lnkhist", "crsp", engine, wrds_id, fix_missing=True)
if ccmxpf_lnkhist:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)")

dsedist = wrds_update("dsedist", "crsp", engine, wrds_id, fix_missing=True)
if dsedist:
    engine.execute("CREATE INDEX ON crsp.dsedist (permno)")
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer")

stocknames = wrds_update("stocknames", "crsp", engine, wrds_id)
if stocknames:
    engine.execute("ALTER TABLE crsp.stocknames ALTER permno TYPE integer")
    engine.execute("ALTER TABLE crsp.stocknames ALTER permco TYPE integer")

dseexchdates = wrds_update("dseexchdates", "crsp", engine, wrds_id)
if dseexchdates:
    engine.execute("ALTER TABLE crsp.dseexchdates ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dseexchdates (permno)")

# Update other data sets
wrds_update("msp500list", "crsp", engine, wrds_id)
wrds_update("ccmxpf_lnkused", "crsp", engine, wrds_id, fix_missing=True)
# wrds_update("fund_names", "crsp", engine, wrds_id, fix_missing=True)
wrds_update("dsf", "crsp", engine, wrds_id, fix_missing=True)
wrds_update("msp500", "crsp", engine, wrds_id, fix_missing=True)

# Fix permissions.
engine.execute("GRANT USAGE ON SCHEMA crsp TO wrds_access")
engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA crsp TO wrds_access")
