import pandas as pd
from io import StringIO
import re, os, subprocess, sys
from time import gmtime, strftime
sys.path.insert(0, '..')
from wrds_fetch import get_row_sql, code_row, set_table_comment

def get_local_process(sas_code):
    p=subprocess.Popen(['sas', '-stdio', '-noterminal', sas_code],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    ret = p.communicate(sas_code)[0].decode('latin1')
    return ret

def sas_to_pandas_local(sas_code):
    """Function that runs SAS code on WRDS server
    and returns a Pandas data frame."""
    p = get_local_process(sas_code)

    df = pd.read_csv(StringIO(p))
    df.columns = map(str.lower, df.columns)

    return(df)

def get_table_sql_local(table_name, fpath, schema, drop="", rename="", return_sql=True):
    sas_template = """
        options nonotes nosource;
		libname tmp '%s';
		* Use PROC CONTENTS to extract the information desired.;
		proc contents data=tmp.%s(drop=%s obs=1 %s) out=schema noprint;
		run;

		proc sort data=schema;
			by varnum;
		run;

		* Now dump it out to a CSV file;
		proc export data=schema(keep=name format formatl formatd length type)
			outfile=stdout dbms=csv;
		run;
    """

    if rename != '':
        rename_str = "rename=(" + rename + ")"
    else:
        rename_str = ""

    if drop != '':
        drop_str = "drop=" + drop
    else:
        drop_str = ""

    sas_code = sas_template % (fpath, table_name, drop_str, rename_str)

    # Run the SAS code on the WRDS server and get the result
    df = sas_to_pandas_local(sas_code)
    df['postgres_type'] = df.apply(code_row, axis=1)

    make_table_sql = "CREATE TABLE " + schema + "." + table_name + " (" + \
                      df.apply(get_row_sql, axis=1).str.cat(sep=", ") + ")"

    # Identify the datetime fields. These need special handling.
    datatimes = df.loc[df['postgres_type']=="timestamp", "name"]
    datetime_cols = [field.lower() for field in datatimes]

    if return_sql:
        return {"sql":make_table_sql, "datetimes":datetime_cols}
    else:
        df['name'] = df['name'].str.lower()
        return df

def get_wrds_process_local(table_name, fpath, schema, wrds_id, drop="", fix_cr = False,
 fix_missing = False, obs="", rename=""):
    if fix_cr:
        fix_missing = True;
        fix_cr_code = """
            array _char _character_;
            do over _char;
                _char = compress(_char, , 'kw');
            end;"""
    else:
        fix_cr_code = ""

    if rename != '':
        rename_str = " rename=(" + rename + ")"
    else:
        rename_str = ""

    if fix_missing or drop != '' or obs != '':
        # If need to fix special missing values, then convert them to
        # regular missing values, then run PROC EXPORT
        if table_name == "dsf":
            dsf_fix = "format numtrd 8.;\n"
        else:
            dsf_fix = ""

        if obs != "":
            obs_str = " obs=" + str(obs)
        else:
            obs_str = ""

        drop_str = "drop=" + drop

        if obs != '' or drop != '' or rename != '':
            sas_table = table_name + "(" + drop_str + obs_str + rename_str + ")"
        else:
            sas_table = table_name

        if table_name == "fund_names":
            fund_names_fix = """
                proc sql;
                    DELETE FROM %s%s
                    WHERE prxmatch('\\D', first_offer_dt) ge 1;
                quit;""" % (wrds_id, table_name)
        else:
            fund_names_fix = ""

        sas_template = """
            options nosource nonotes;
			libname tmp '%s';
            * Fix missing values;
            data %s%s;
                set tmp.%s;

                * dsf_fix;
                %s

                * fix_cr_code;
                %s

                array allvars _numeric_ ;

                do over allvars;
                  if missing(allvars) then allvars = . ;
                end;
            run;

            * fund_names_fix;
            %s

            proc export data=%s%s outfile=stdout dbms=csv;
            run;"""
        sas_code = sas_template % (fpath, schema, table_name, sas_table, dsf_fix,
                                   fix_cr_code, fund_names_fix, schema, table_name)
    else:

        sas_template = """
            options nosource nonotes;
			libname tmp '%s';
            proc export data=tmp.%s(%s) outfile=stdout dbms=csv;
            run;"""

        sas_code = sas_template % (fpath, table_name, rename_str)

    p = get_local_process(sas_code)
    return(p)

def wrds_to_pg_local(table_name, fpath, schema, engine, wrds_id,
               fix_missing=False, fix_cr=False, drop="", obs="", rename=""):

    make_table_data = get_table_sql_local(table_name=table_name, fpath=fpath, schema=schema, drop=drop, rename=rename)

    #res = engine.execute("CREATE SCHEMA IF NOT EXISTS " + schema)
    res = engine.execute("DROP TABLE IF EXISTS " + schema + "." + table_name + " CASCADE")
    res = engine.execute(make_table_data["sql"])

    now = strftime("%H:%M:%S", gmtime())
    print("Beginning file import at %s." % now)
    print("Importing data into %s.%s" % (schema, table_name))
    p = get_wrds_process_local(table_name=table_name, fpath=fpath, schema=schema, wrds_id=wrds_id,
				drop=drop, fix_cr=fix_cr, fix_missing = fix_missing, obs=obs, rename=rename)

    res = wrds_process_to_pg_local(table_name, schema, engine, p)
    now = strftime("%H:%M:%S", gmtime())
    print("Completed file import at %s." % now)

    for var in make_table_data["datetimes"]:
        print("Fixing %s" % var)
        sql = r"""
            ALTER TABLE "%s"."%s"
            ALTER %s TYPE timestamp
            USING regexp_replace(%s, '(\d{2}[A-Z]{3}\d{4}):', '\1 ' )::timestamp""" % (schema, table_name, var, var)
        engine.execute(sql)

    return res

def wrds_process_to_pg_local(table_name, schema, engine, p):
    # The first line has the variable names ...
    var_names = p[:p.find('\n')].rstrip().lower().split(sep=",")

    # ... the rest is the data
    copy_cmd =  "COPY " + schema + "." + table_name + " (" + ", ".join(var_names) + ")"
    copy_cmd += " FROM STDIN CSV ENCODING 'latin1'"

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.copy_expert(copy_cmd, StringIO(p[p.find('\n') + 1:]))
        cursor.close()
        connection.commit()
    finally:
        connection.close()

    return True

def wrds_update_local(table_name, fpath, schema, engine, wrds_id, fix_missing=False, fix_cr=False, drop="", obs="", rename=""):

    wrds_to_pg_local(table_name=table_name, fpath=fpath, schema=schema, engine=engine,
		wrds_id=wrds_id, fix_missing=fix_missing, fix_cr=fix_cr, drop=drop, obs=obs, rename=rename)

    comment = 'Updated on ' + strftime("%Y-%m-%d %H:%M:%S", gmtime())

    set_table_comment(table_name, schema, comment, engine)

    sql = r"""
        ALTER TABLE "%s"."%s" OWNER TO %s""" % (schema, table_name, schema)
    engine.execute(sql)

    sql = r"""
        GRANT SELECT ON "%s"."%s"  TO %s_access""" % (schema, table_name, schema)
    engine.execute(sql)

    return True
