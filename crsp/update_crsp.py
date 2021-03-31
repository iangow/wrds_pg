#!/usr/bin/env python3
from sqlalchemy import create_engine

from wrds2pg import wrds_update, get_process, wrds_process_to_pg, \
    run_file_sql, make_engine, wrds_id, set_table_comment
from time import gmtime, strftime

engine = make_engine()

# Update Treasury yield table crsp.tfz_ft
# From WRDS:
# The error is correct, the table "tfz_ft," does not exist. Behind the scenes this web
# query form is joining two tables on the fly. The tables this query is joining are
# "crsp.tfz_idx" and either "crsp.tfz_dly_ft" or "crsp.tfz_mth_ft" (depending on if
# you want daily or monthly data) by the variable "kytreasnox."

# Here are some links to the information about these tables:
# https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77140
# https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77137
# https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77147
tfz_idx = wrds_update("tfz_idx", "crsp",
                      col_types = {'kytreasnox':'integer', 
                                   'ttermtype':'integer',
                                   'rdtreasno':'integer'})
tfz_dly_ft = wrds_update("tfz_dly_ft", "crsp",
            col_types = {'tdyearstm':'float8', 'tdduratn':'float8',
                         'tdytm':'float8', 'tdbid':'float8',
                         'tdask':'float8', 'tdnomprc':'float8',
                         'tdaccint':'float8', 'tdretadj':'float8'})
if tfz_idx or tfz_dly_ft:

    sql = """
        DROP TABLE IF EXISTS crsp.tfz_ft;
        CREATE TABLE crsp.tfz_ft
        AS
        (SELECT kytreasnox, tidxfam, ttermtype, ttermlbl, caldt, rdtreasno, rdcrspid
        FROM crsp.tfz_idx
        INNER JOIN crsp.tfz_dly_ft
        USING (kytreasnox))
        """
    engine.execute(sql)
    engine.execute("ALTER TABLE crsp.tfz_ft OWNER TO crsp")
    engine.execute("GRANT SELECT ON crsp.tfz_ft TO crsp_access")
    # Add comments
    sql = "Created using update_crsp.py ON " + strftime("%Y-%m-%d %H:%M:%S", gmtime())
    set_table_comment("tfz_ft", "crsp", sql, engine)

mse = wrds_update("mse", "crsp", fix_missing=True)

# Update monthly data
msf = wrds_update("msf", "crsp", fix_missing=True)
if msf:
    engine.execute("CREATE INDEX ON crsp.msf (permno, date)")

msi = wrds_update("msi", "crsp")

msedelist = wrds_update("msedelist", "crsp", fix_missing=True)

mport = wrds_update("mport1", "crsp")
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

    p = get_process(sas_code, wrds_id)
    res = wrds_process_to_pg("ermport", "crsp", engine, p)
    run_file_sql("crsp/crsp_make_ermport1.sql", engine)
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

if mport or msf or msi or msedelist:
    run_file_sql("crsp/crsp_make_mrets.sql", engine)
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
dsf = wrds_update("dsf", "crsp", fix_missing=True)
if dsf:
    engine.execute("ALTER TABLE crsp.dsf ALTER permno TYPE integer")
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON crsp.dsf (permno, date)")

dsi = wrds_update("dsi", "crsp")
if dsi:
    engine.execute("CREATE INDEX ON crsp.dsi (date)")
    run_file_sql("crsp/make_trading_dates.sql", engine)
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

dsedelist = wrds_update("dsedelist", "crsp", fix_missing=True)
if dsedelist:
    engine.execute("ALTER TABLE crsp.dsedelist ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dsedelist (permno)")

dport = wrds_update("dport1", "crsp")

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

    p = get_process(sas_code, wrds_id)
    res = wrds_process_to_pg("erdport", "crsp", engine, p)

    run_file_sql("crsp/crsp_make_erdport1.sql", engine)
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

if dport or dsf or dsi or dsedelist:
    run_file_sql("crsp/crsp_make_rets_alt.sql", engine)
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

ccmxpf_linktable = wrds_update("ccmxpf_linktable", "crsp", fix_missing=True)
if ccmxpf_linktable:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermno TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER lpermco TYPE integer")
    engine.execute("ALTER TABLE crsp.ccmxpf_linktable ALTER usedflag TYPE integer")

ccmxpf_lnkhist = wrds_update("ccmxpf_lnkhist", "crsp", fix_missing=True)
if ccmxpf_lnkhist:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)")

dsedist = wrds_update("dsedist", "crsp", fix_missing=True)
if dsedist:
    engine.execute("CREATE INDEX ON crsp.dsedist (permno)")
    engine.execute("ALTER TABLE crsp.dsedist ALTER permno TYPE integer")

stocknames = wrds_update("stocknames", "crsp")
if stocknames:
    engine.execute("ALTER TABLE crsp.stocknames ALTER permno TYPE integer")
    engine.execute("ALTER TABLE crsp.stocknames ALTER permco TYPE integer")

dseexchdates = wrds_update("dseexchdates", "crsp")
if dseexchdates:
    engine.execute("ALTER TABLE crsp.dseexchdates ALTER permno TYPE integer;")
    engine.execute("CREATE INDEX ON crsp.dseexchdates (permno)")

# Update other data sets
wrds_update("msp500list", "crsp")
wrds_update("ccmxpf_lnkused", "crsp", fix_missing=True)
# wrds_update("fund_names", "crsp", fix_missing=True)

wrds_update("dsp500", "crsp")
wrds_update("dsp500p", "crsp")
wrds_update("msp500", "crsp")
wrds_update("msp500p", "crsp")
wrds_update("mcti", "crsp")
wrds_update("mcti_corr", "crsp")
wrds_update("msedist", "crsp")
wrds_update("mseshares", "crsp")
wrds_update("comphist", "crsp")

engine.dispose()
