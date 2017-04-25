# Run the SAS code on the WRDS server and get the result
import subprocess
import pandas as pd
from io import StringIO

def get_process(sas_code, wrds_id):

    import paramiko
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.connect('wrds-cloud.wharton.upenn.edu',
                   username=wrds_id, compress=True)
    stdin, stdout, stderr = client.exec_command("qsas -stdio -noterminal")
    stdin.write(sas_code)
    stdin.close()

    channel = stdout.channel
    # indicate that we're not going to write to that channel anymore
    channel.shutdown_write()
    return stdout

import re

def code_row(row):

    """A function to code PostgreSQL data types using output from SAS's PROC CONTENTS"""

    format_ = row['format']
    formatd = row['formatd']
    formatl = row['formatl']
    col_type = row['type']

    if col_type==2:
        return 'text'

    if not pd.isnull(format_):
        if re.search(r'datetime', format_, re.I):
            return 'timestamp'
        elif format_ =='TIME8.' or re.search(r'time', format_, re.I):
            return "time"
        elif re.search(r'(date|yymmdd)', format_, re.I):
            return "date"

    if formatd != 0:
        return 'float8'
    if formatd == 0 and formatl != 0:
        return 'int8'
    if formatd == 0 and formatl == 0:
        return 'float8'
    else:
        return 'text'

################################################
# 1. Get format of variables on WRDS table     #
################################################
# SAS code to extract information about the datatypes of the SAS data.
# Note that there are some date formates that don't work with this code.
def get_row_sql(row):
    return row['name'].lower() + ' ' + row['postgres_type']

def sas_to_pandas(sas_code, wrds_id):

    p = get_process(sas_code, wrds_id)
    df = pd.read_csv(StringIO(p.read().decode('latin1')))
    df.columns = map(str.lower, df.columns)
    p.close()

    return(df)

def get_table_sql(schema, table_name, wrds_id, drop="", rename="", return_sql=True):
    sas_template = """
        options nonotes nosource;

        * Use PROC CONTENTS to extract the information desired.;
        proc contents data=%s.%s(drop=%s obs=1 %s) out=schema noprint;
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

    sas_code = sas_template % (schema, table_name, drop_str, rename_str)

    # Run the SAS code on the WRDS server and get the result
    df = sas_to_pandas(sas_code, wrds_id)
    df['postgres_type'] = df.apply(code_row, axis=1)
    make_table_sql = "CREATE TABLE " + schema + "." + table_name + " (" +                 df.apply(get_row_sql, axis=1).str.cat(sep=", ") + ")"
    if return_sql:
        return make_table_sql
    else:
        return df

def get_wrds_process(schema, table_name, wrds_id, drop="",
                     fix_cr = False, fix_missing = False, obs="",
                     rename=""):

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
        rename_str = "rename=(" + rename + ")"
    else:
        rename_str = ""

    if fix_missing or drop != '' or obs != '':
        # If need to fix special missing values, then convert them to
        # regular missing values, then run PROC EXPORT
        if table_name == "dsf":
            dsf_fix = "format numtrd 8.;\n"
        else:
            dsf_fix = ""

        drop_str = "drop=" + drop

        if obs != '' or drop != '' or rename != '':
            sas_table = table_name + "(" + drop_str + obs_str + rename_str + ")"
        else:
            sas_table = table_name

        if table_name == "fund_names":
            fund_names_fix = """
                proc sql;
                    DELETE FROM pwd.%s%s
                    WHERE prxmatch('\\D', first_offer_dt) ge 1;
                quit;""" % (wrds_id, table_name)
        else:
            fund_names_fix = ""

        sas_template = """
            options nosource nonotes;

            libname pwd '/sastemp';

            * Fix missing values;
            data pwd.%s%s;
                set %s.%s;

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

            proc export data=pwd.%s%s outfile=stdout dbms=csv;
            run;"""
        sas_code = sas_template % (schema, table_name, schema, sas_table, dsf_fix,
                                   fix_cr_code, fund_names_fix, schema, table_name)
    else:

        sas_template = """
            options nosource nonotes;

            proc export data=%s.%s outfile=stdout dbms=csv;
            run;"""

        sas_code = sas_template % (schema, table_name)

    p = get_process(sas_code, wrds_id)
    return(p)

def wrds_to_pandas(schema, table_name, wrds_id):

    p = get_wrds_process(schema, table_name, wrds_id)
    df = pd.read_csv(StringIO(p.read().decode('latin1')))
    df.columns = map(str.lower, df.columns)
    p.close()

    return(df)

def wrds_to_pg(schema, table_name, engine, wrds_id, drop="", rename="", obs="",
               fix_cr = False, fix_missing = False):

    make_table_sql = get_table_sql(schema, table_name, wrds_id=wrds_id,
                                   drop=drop, rename=rename)
    res = engine.execute("CREATE SCHEMA IF NOT EXISTS " + schema)
    res = engine.execute("DROP TABLE IF EXISTS " + schema + "." + table_name + " CASCADE")
    res = engine.execute(make_table_sql)

    p = get_wrds_process(schema, table_name, wrds_id, drop=drop, fix_cr = fix_cr,
                         fix_missing = fix_missing, obs=obs, rename=rename)

    # The first line has the variable names ...
    var_names = p.readline().rstrip().lower().split(sep=",")

    # ... the rest is the data
    copy_cmd =  "COPY " + schema + "." + table_name + " (" + ", ".join(var_names) + ")"
    copy_cmd += "FROM STDIN CSV ENCODING 'latin1'"

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.copy_expert(copy_cmd, p)
        cursor.close()
        connection.commit()
    finally:
        connection.close()
        p.close()
    return True
