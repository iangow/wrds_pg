#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

from wrds2pg import wrds2pg
from time import gmtime, strftime

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
'''
tfz_idx = wrds2pg.wrds_update("tfz_idx", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
tfz_dly_ft = wrds2pg.wrds_update("tfz_dly_ft", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
tfz_idx=True
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
    # Add comments here
    sql = "COMMENT ON TABLE crsp.tfz_ft IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

mse = wrds2pg.wrds_update("mse", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)

# Update monthly data
msf = wrds2pg.wrds_update("msf", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if msf:
    engine.execute("CREATE INDEX ON crsp.msf (permno, date)")

msi = wrds2pg.wrds_update("msi", "crsp", engine=engine, wrds_id=wrds_id)

msedelist = wrds2pg.wrds_update("msedelist", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)

mport = wrds2pg.wrds_update("mport1", "crsp", engine=engine, wrds_id=wrds_id)
mport=True
if mport:
    print("Getting ermport1")
    # from wrds_fetch import get_process, wrds_process_to_pg

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

    p = wrds2pg.get_process(sas_code, wrds_id)
    res = wrds2pg.wrds_process_to_pg("ermport", "crsp", engine, p)
    wrds2pg.run_file_sql("crsp_make_ermport1.sql", engine)
    engine.execute("ALTER TABLE crsp.ermport OWNER TO crsp")
    engine.execute("GRANT SELECT ON crsp.ermport TO crsp_access")
    ### Add comments here
    sql = "COMMENT ON TABLE crsp.ermport IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

if msi:
    engine.execute("CREATE INDEX ON crsp.msi (  date)")
mport=True
if mport or msf or msi or msedelist:
    wrds2pg.run_file_sql("crsp_make_mrets.sql", engine)
    # Add comments here
    sql = "COMMENT ON TABLE crsp.mrets IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

# Update daily data
dsf = wrds2pg.wrds_update("dsf", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if dsf:
    engine.execute("ALTER TABLE crsp.dsf ALTER permno TYPE integer")
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON crsp.dsf (permno, date)")

dsi = wrds2pg.wrds_update("dsi", "crsp", engine=engine, wrds_id=wrds_id)
'''
dsi=True
if dsi:
    # engine.execute("CREATE INDEX ON crsp.dsi (date)")
    # wrds2pg.run_file_sql("make_trading_dates.sql", engine)
    # Add comments here
    sql1 = "COMMENT ON TABLE crsp.trading_dates IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    sql2 = "COMMENT ON TABLE crsp.anncdates IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql1)
        res = connection.execute(sql2)
        trans.commit()
    except:
        trans.rollback()
        raise

'''
dsedelist = wrds2pg.wrds_update("dsedelist", "crsp", engine=engine, wrds_id=wrds_id,
                            fix_missing=True)
if dsedelist:
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dsedelist (permno)")

dport = wrds2pg.wrds_update("dport1", "crsp", engine=engine, wrds_id=wrds_id)

if dport:
    engine.execute("ALTER TABLE crsp.dport1 ALTER permno TYPE integer")

if dport:
    print("Getting ermport1")
    # from wrds_fetch import get_process, wrds_process_to_pg

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

    p = wrds2pg.get_process(sas_code, wrds_id)
    res = wrds2pg.wrds_process_to_pg("erdport", "crsp", engine, p)

    wrds2pg.run_file_sql("crsp_make_erdport1.sql", engine)
    engine.execute("CREATE INDEX ON crsp.dport1 (permno, date)")
    engine.execute("ALTER TABLE crsp.erdport OWNER TO crsp")
    engine.execute("GRANT SELECT ON TABLE crsp.erdport TO crsp_access")
    #### Add comments here
    sql = "COMMENT ON TABLE crsp.erdport IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise
```

if dport or dsf or dsi or dsedelist:
    wrds2pg.run_file_sql("crsp_make_rets_alt.sql", engine)
    #### Add comments here
    sql = "COMMENT ON TABLE crsp.rets IS 'Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

ccmxpf_linktable = wrds2pg.wrds_update("ccmxpf_linktable", "crsp", engine=engine, wrds_id=wrds_id,
                                fix_missing=True)
if ccmxpf_linktable:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermno TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermco TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER usedflag TYPE integer")

ccmxpf_lnkhist = wrds2pg.wrds_update("ccmxpf_lnkhist", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if ccmxpf_lnkhist:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)")

dsedist = wrds2pg.wrds_update("dsedist", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
if dsedist:
    engine.execute("CREATE INDEX ON crsp.dsedist (permno)")
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer")

stocknames = wrds2pg.wrds_update("stocknames", "crsp", engine=engine, wrds_id=wrds_id)
if stocknames:
    engine.execute("ALTER TABLE crsp.stocknames ALTER permno TYPE integer")
    engine.execute("ALTER TABLE crsp.stocknames ALTER permco TYPE integer")

dseexchdates = wrds2pg.wrds_update("dseexchdates", "crsp", engine=engine, wrds_id=wrds_id)
if dseexchdates:
    engine.execute("ALTER TABLE crsp.dseexchdates ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dseexchdates (permno)")

# Update other data sets
wrds2pg.wrds_update("msp500list", "crsp", engine=engine, wrds_id=wrds_id)
wrds2pg.wrds_update("ccmxpf_lnkused", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)
# wrds2pg.wrds_update("fund_names", "crsp", engine=engine, wrds_id=wrds_id, fix_missing=True)

wrds2pg.wrds_update("msp500", "crsp", engine=engine, wrds_id=wrds_id)
wrds2pg.wrds_update("msp500p", "crsp", engine=engine, wrds_id=wrds_id)
wrds2pg.wrds_update("mcti", "crsp", engine=engine, wrds_id=wrds_id)
wrds2pg.wrds_update("mcti_corr", "crsp", engine=engine, wrds_id=wrds_id)
wrds2pg.wrds_update("msedist", "crsp", engine=engine, wrds_id=wrds_id)
wrds2pg.wrds_update("mseshares", "crsp", engine=engine, wrds_id=wrds_id)

# Fix permissions.
engine.execute("GRANT USAGE ON SCHEMA crsp TO wrds_access")
engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA crsp TO wrds_access")
'''
engine.dispose()
