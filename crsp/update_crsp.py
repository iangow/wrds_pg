#!/usr/bin/env python3
from sqlalchemy import create_engine 
from wrds2pg import wrds_update, get_process, wrds_process_to_pg
from wrds2pg import run_file_sql, make_engine, wrds_id, set_table_comment
from wrds2pg import process_sql
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
                         col_types = {'kytreasnox':'integer',
                                      'tdyearstm':'float8', 
                                      'tdduratn':'float8',
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
    process_sql(sql, engine)
    process_sql("ALTER TABLE crsp.tfz_ft OWNER TO crsp", engine)
    process_sql("GRANT SELECT ON crsp.tfz_ft TO crsp_access", engine)
    # Add comments
    now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    comment = f"Created using update_crsp.py ON {now}." 
    set_table_comment("tfz_ft", "crsp", comment, engine)

mse = wrds_update("mse", "crsp", fix_missing=True)

# Update monthly data
msf = wrds_update("msf", "crsp", fix_missing=True,
                  col_types = {'permno':'integer', 'permco':'integer'})
if msf:
    process_sql("CREATE INDEX ON crsp.msf (date)", engine)
    process_sql("CREATE INDEX ON crsp.msf (permno, date)", engine)
    process_sql("CREATE INDEX ON crsp.msf (permno)", engine)
    process_sql("CREATE INDEX ON crsp.msf (permco)", engine)

msi = wrds_update("msi", "crsp")

msedelist = wrds_update("msedelist", "crsp", fix_missing=True)

ermport1 = wrds_update("ermport1", "crsp", fix_missing=True,
                        col_types = {'permno':'integer', 'capn':'integer'})

if msi:
    process_sql("CREATE INDEX ON crsp.msi (date)", engine)

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
                  col_types = {'permno':'integer', 'permco': 'integer'})
if dsf:
    process_sql("SET maintenance_work_mem='1999MB'", engine)
    process_sql("CREATE INDEX ON crsp.dsf (permno, date)", engine)
    process_sql("CREATE INDEX ON crsp.dsf (permco)", engine)
    process_sql("CREATE INDEX ON crsp.dsf (permno)", engine)

dsi = wrds_update("dsi", "crsp")
if dsi:
    process_sql("CREATE INDEX ON crsp.dsi (date)", engine)
    run_file_sql("crsp/make_trading_dates.sql", engine)
    # Add comments here
    sql1 = "COMMENT ON TABLE crsp.trading_dates IS " + \
                "'Created using update_crsp.py ON " + \
                strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    sql2 = "COMMENT ON TABLE crsp.anncdates IS " + \
            "'Created using update_crsp.py ON " + \
            strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    process_sql(sql1, engine)
    process_sql(sql2, engine)

dsedelist = wrds_update("dsedelist", "crsp", fix_missing=True,
                        col_types = {'permno':'integer', 'permco': 'integer'})
if dsedelist:
    process_sql("CREATE INDEX ON crsp.dsedelist (permno)", engine)

erdport1 = wrds_update("erdport1", "crsp", fix_missing = True,
                       col_types = {'permno':'integer', 'capn': 'integer'})

if erdport1:
    process_sql("CREATE INDEX ON crsp.erdport1 (permno, date)", engine)
    
if erdport1 or dsf or dsi or dsedelist:
    run_file_sql("crsp/crsp_make_rets.sql", engine)
    #### Add comments here
    sql = "COMMENT ON TABLE crsp.rets IS 'Created using update_crsp.py ON " + \
            strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
    process_sql(sql, engine)

ccmxpf_linktable = wrds_update("ccmxpf_linktable", "crsp", fix_missing=True,
                                col_types = {'lpermno':'integer', 
                                             'lpermco': 'integer',
                                             'usedflag': 'integer'})
if ccmxpf_linktable:
    process_sql("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)", engine)
    process_sql("CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)", engine)
    process_sql("CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)", engine)
    
ccmxpf_lnkhist = wrds_update("ccmxpf_lnkhist", "crsp", fix_missing=True,
                                 col_types = {'lpermno':'integer', 
                                              'lpermco': 'integer'})
if ccmxpf_lnkhist:
    process_sql("CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)", engine)
    process_sql("CREATE INDEX ON crsp.ccmxpf_lnkhist (lpermno)", engine)

dsedist = wrds_update("dsedist", "crsp", fix_missing=True,
                      col_types = {'permno':'integer',
                                   'permco':'integer'})
if dsedist:
    process_sql("CREATE INDEX ON crsp.dsedist (permno)", engine)

dse = wrds_update("dse", "crsp", fix_missing=True,
                      col_types = {'permno':'integer',
                                   'permco':'integer'})
if dse:
    process_sql("CREATE INDEX ON crsp.dse (permno)", engine)

stocknames = wrds_update("stocknames", "crsp",
                          col_types = {'permno':'integer', 
                                       'permco': 'integer'})
                                            
dseexchdates = wrds_update("dseexchdates", "crsp",
                           col_types = {'permno':'integer', 
                                        'permco': 'integer'})
if dseexchdates:
    process_sql("CREATE INDEX ON crsp.dseexchdates (permno)", engine=engine)

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
