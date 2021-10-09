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

ermport1 = wrds_update("ermport1", "crsp", fix_missing=True,
                        col_types = {'permno':'integer', 'capn':'integer'})

if msi:
    engine.execute("CREATE INDEX ON crsp.msi (date)")

if ermport1 or msf or msi or msedelist:
    run_file_sql("crsp/crsp_make_mrets.sql", engine)
    # Add comments here
    sql = "COMMENT ON TABLE crsp.mrets IS 'Created using update_crsp.py ON " + \
            strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

# Update daily data
dsf = wrds_update("dsf", "crsp", fix_missing=True,
                  col_types = {'permno':'integer', 'permno': 'integer'})
if dsf:
    engine.execute("SET maintenance_work_mem='1999MB'")
    engine.execute("CREATE INDEX ON crsp.dsf (permno, date)")

dsi = wrds_update("dsi", "crsp")
if dsi:
    engine.execute("CREATE INDEX ON crsp.dsi (date)")
    run_file_sql("crsp/make_trading_dates.sql", engine)
    # Add comments here
    sql1 = "COMMENT ON TABLE crsp.trading_dates IS " + \
                "'Created using update_crsp.py ON " + \
                strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    sql2 = "COMMENT ON TABLE crsp.anncdates IS " + \
            "'Created using update_crsp.py ON " + \
            strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql1)
        res = connection.execute(sql2)
        trans.commit()
    except:
        trans.rollback()
        raise

dsedelist = wrds_update("dsedelist", "crsp", fix_missing=True,
                        col_types = {'permno':'integer', 'permco': 'integer'})
if dsedelist:
    engine.execute("CREATE INDEX ON crsp.dsedelist (permno)")

erdport1 = wrds_update("erdport1", "crsp", fix_missing = True,
                       col_types = {'permno':'integer', 'capn': 'integer'})

if erdport1:
    engine.execute("CREATE INDEX ON crsp.dport1 (permno, date)")
    
if erdport1 or dsf or dsi or dsedelist:
    run_file_sql("crsp/crsp_make_rets.sql", engine)
    #### Add comments here
    sql = "COMMENT ON TABLE crsp.rets IS 'Created using update_crsp.py ON " + \
            strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise

ccmxpf_linktable = wrds_update("ccmxpf_linktable", "crsp", fix_missing=True,
                                col_types = {'lpermno':'integer', 
                                             'lpermco': 'integer',
                                             'usedflag': 'integer'})
if ccmxpf_linktable:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)")
    engine.execute("CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)")
    
    ccmxpf_lnkhist = wrds_update("ccmxpf_lnkhist", "crsp", fix_missing=True,
                                 col_types = {'lpermno':'integer', 
                                              'lpermco': 'integer'})
if ccmxpf_lnkhist:
    engine.execute("CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)")

dsedist = wrds_update("dsedist", "crsp", fix_missing=True,
                      col_types = {'permno':'integer'})
if dsedist:
    engine.execute("CREATE INDEX ON crsp.dsedist (permno)")

stocknames = wrds_update("stocknames", "crsp",
                          col_types = {'permno':'integer', 
                                       'permco': 'integer'})
                                            
dseexchdates = wrds_update("dseexchdates", "crsp",
                           col_types = {'permno':'integer', 
                                        'permco': 'integer'})
if dseexchdates:
    engine.execute("CREATE INDEX ON crsp.dseexchdates (permno)")

# Update other data sets
wrds_update("msp500list", "crsp")
wrds_update("ccmxpf_lnkused", "crsp", fix_missing=True)

wrds_update("dsp500", "crsp")
wrds_update("dsp500p", "crsp")
wrds_update("msp500", "crsp")
wrds_update("msp500p", "crsp")
wrds_update("mcti", "crsp")
wrds_update("mcti_corr", "crsp")
wrds_update("msedist", "crsp")
wrds_update("mseshares", "crsp")
wrds_update("comphist", "crsp", fix_missing=True)

engine.dispose()
