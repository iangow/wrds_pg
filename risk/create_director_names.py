#!/usr/bin/enn python3
import psycopg2 as pg
import pandas as pd
from pandas.io.sql import read_sql
from sqlalchemy import create_engine, text
import os
import re
import json

host = os.getenv("PGHOST")
dbname = os.getenv("PGDATABASE")

engine = create_engine("postgresql://" + host + "/" + dbname)
conn = engine.connect()

sql = r"""SELECT DISTINCT itemdesc
    FROM risk.vavoteresults AS a
    -- INNER JOIN risk.issrec
    -- USING (itemonagendaid)
    WHERE a.issagendaitemid IN ('S0299', 'M0299', 'M0201', 'S0201', 'M0225')
        AND itemdesc ~ '^Elect' 
        AND voteresult != 'Withdrawn'
        AND NOT agendageneraldesc ILIKE '%%inactive%%'
"""

df = read_sql(sql, con=conn)



def extract_name(itemdesc):
    itemdesc_orig = itemdesc
    
    prefix = name = first_name = last_name =  None
    suffix = last_name_prefix = middle_initial = None
    
    misspellings = r"irector|Dirctor|Director Director|Directror|Driector|Directo"
    regex = r"\b(" + misspellings + r")\b"
    itemdesc = re.sub(regex, "Director", itemdesc)
    itemdesc = re.sub(r"\bTurstee", "Trustee", itemdesc)
    itemdesc = re.sub(r"\*", "", itemdesc) # Remove asterisks
    
    # Remove any multiple spaces
    itemdesc = re.sub(r"\s{2,}", " ", itemdesc) 
    itemdesc = re.sub(r"Elect Director Robert W. Wo., Jr.", "Elect Director Robert W. Wo, Jr.", itemdesc)
    itemdesc = re.sub(r"Elect Chen, Chin-Hsin \(Fred\) as Director", "Elect Director Chin-Hsin (Fred) Chen", itemdesc)
    itemdesc = re.sub(r"Elect Pan, I-Ming \(Robin\) as Director", "Elect Director I-Ming (Robin) Pan", itemdesc)
    itemdesc = re.sub(r"Elect Huang, Jun-Tse \(Walter\) as Director", "Elect Director Jun-Tse (Walter) Huang", itemdesc)
    itemdesc = re.sub(r" Jr\., M\.D\.,?$", ", Jr. MD", itemdesc) 
    
    # Change alternative forms
    regex = r"^(Elector Director|Elect\s+Director:? |Election Of Director |Elect\s?Director|Election\s?Director |Election Of Director:? )"
    itemdesc = re.sub(regex, "Elect Director ", itemdesc)
    
    regex = r"\bAs A Director Of\b.*$"
    itemdesc = re.sub(regex, "", itemdesc, flags = re.I)
    
    # Elect  Director Green
    itemdesc = re.sub(r"([A-Z]\.)([A-Z])", r"\1 \2", itemdesc)
    itemdesc = re.sub(r"Siddharth N. \"Bobby\" Mehta", "Siddharth N. Mehta", itemdesc)
    itemdesc = re.sub(r"C.L.Miller", "C.L. Miller", itemdesc)
    itemdesc = re.sub(r"Charles F. \"Charlie\" Parker, Jr.", "Charles F. Parker, Jr.", itemdesc)
    itemdesc = re.sub(r"\s*as Class I Director$", "", itemdesc)
    itemdesc = re.sub(r"Il, Yung Kim", "Yung Kim Il", itemdesc)
    itemdesc = re.sub(r"Elect\s+(.*)\sas (a )?Director.*$", r"Elect Director \1", itemdesc)
    itemdesc = re.sub(r"as .*Director", "", itemdesc)
    itemdesc = re.sub(r" (as|by Holders of) (Class (A|B) )?(Common )?Stock", "", itemdesc, flags = re.I)
    itemdesc = re.sub(r"Philip R, Roth", "Philip R. Roth", itemdesc)
    itemdesc = re.sub(r"The Duke Of", "Duke", itemdesc, flags = re.I)
    itemdesc = re.sub(r"(The (Right|Rt\.)? )?Hon(ou?rable|\.) ", "Rt. Hon. ", itemdesc, flags = re.I)
    itemdesc = re.sub(r"Elect Director Norman S. Edelcup Elect.*", "Elect Director Norman S. Edelcup", itemdesc, flags = re.I)
    itemdesc = re.sub(r"Elect Director ohn\s+", "Elect Director John ", itemdesc)
    
    itemdesc = re.sub(r"[\(\)]", "", itemdesc)
    # Look for forms like "Elect Ian D. Gow" (i.e., no words other than "elect" and the name
    if re.search(r"^Elect(?! Director)", itemdesc):
      if not re.search(r"\b(Auditors|Trust|Company|Members|Inc\.|of|as|to)\b", itemdesc):
        itemdesc = re.sub(r"Elect (.*)", r"Elect Director \1", itemdesc)
    
    # Pull out text after "Elect director";
    # if the word "and" appears, delete the observation
    # as there are multiple names.
    m1 = re.search(r"(?:Elect\s+Directors?)\s+(.+)$", itemdesc, flags = re.I)    
    m2 = re.search(r"^(?:Elect Director)\s+([A-Za-z]+)$", itemdesc)      
  
    # if m1:
        # print("m1", itemdesc_orig) 
    
    if m2:
        # print("m2", itemdesc_orig) 
        return json.dumps({'name' : m2.group(1), 
                           'first_name' : None, 
                           'middle_initial' : None,
                           'last_name' : m2.group(1), 
                           'suffix' : None, 
                           'prefix' : None})
                 
    if not re.search(r"\band\b", itemdesc, flags = re.I) and m1:
        # print("m1 & no and: ", itemdesc_orig) 
        name = m1.group(1)
        
        # Remove leading spaces
        name = re.sub(r"^\s+", "", name)
        
        # Some suffixes are not always separated by a comma, but we can be confident that
        # they're suffixes. Pull these out too.
        m3 = re.search(r"^(.*)\s+(.*?)\s+(Jr\.|M\.D\.|JR\s?\.?|PH\.?D\.?|II|III|IV|V|VI|M\.?D\.?|\(RET(\.|ired)?\)|3D|CBE)$", name, flags = re.I)
        # if m3:
            # print("m3", itemdesc_orig) 
        
        m5 = re.search(r",", name)
        if m5:
            # print("m5", itemdesc_orig) 
            # If there's a comma, put the part after the first comma into a suffix
            m4 = re.search(r"(.+?)\s+([-\w']*?)\s?,\s?(.*)\s?$", name)
            
            if m4:
                # print("m4", itemdesc_orig) 
                first_name = m4.group(1)
                last_name  = m4.group(2)
                suffix = m4.group(3)
            
                
        elif m3:
            # print("m3", itemdesc_orig) 
            first_name = m3.group(1)
            last_name  = m3.group(2)
            suffix = m3.group(3)
            
            
        elif name:
            
            name = re.sub(r"\s+$", "", name)
            
            # If there's no suffix...
            m = re.search(r"^(.+)\s+(.+?)$", name)
            if m:
                first_name = m.group(1)
                last_name  = m.group(2)
        else:
          
          return json.dumps({'name' : None, 
                'first_name' : 'Ian', 
                'middle_initial' : None, 
                'last_name' : 'Gow', 
                'suffix' : None, 
                'prefix' : None})
    
        # Pull out prefixes like Mr, Dr, etc.
        # print("first_name", first_name) 
        if first_name:
            m5 = re.search(r"^((?:Amb\.|Ambassador|(?:Rear|Vice )?(?:Adm\.|Admiral)|RADM|(?:Maj\. |Major )?Gen\.)\.? )?(.*)$", first_name, flags = re.I)
            if m5:
            
                try:
                    # print("m5_a", itemdesc_orig) 
                    prefix = m5.group(1)
                    # print("m5_b", itemdesc_orig) 
                    first_name = m5.group(2)
                except TypeError:
                    print("TypeError:", itemdesc_orig)  
            #  print("pre_m6", itemdesc_orig)  
            m6 = re.search(r"^((?:(?:Lieutenant |Major )?General)\.? )?(.*)$", first_name, flags = re.I)
            # if m6:
                # print("m6", itemdesc_orig) 
            
            if not prefix and m6:
                # print("m6", itemdesc_orig) 
                prefix = m6.group(1)
                first_name = m6.group(2)
        
        
            m7 = re.search(r"^((?:Lt Gen|Hon\.|Prof\.|Professor|Rev\.|Rt\. Hon\.?|Sir|Dr|Mr|Mrs|Ms)\.? )?(.*)$", first_name, flags = re.I)
            #if m7:
            #    print("m7", itemdesc_orig) 
            if not prefix and m7:
                prefix = m7.group(1)
                first_name = m7.group(2)
        
            m8 = re.search(r"^(Sen\. |Senator )(.*)$", first_name, flags = re.I)
            # if m8:
                # print("m8", itemdesc_orig)
            if not prefix and m8:
                prefix = m8.group(1)
                first_name = m8.group(2)
                
            # print("first_name:", itemdesc_orig)
            first_name = first_name.strip()
        
        
            # Remove last-name prefixes from first names
            m9 = re.search(r"(.+?)((?:\s[a-z]+)+)$", first_name)
            if m9:
                # print("m9", itemdesc_orig)
                first_name = m9.group(1)
                last_name_prefix = m9.group(2)
                if last_name_prefix:
                    last_name = last_name_prefix.strip() + ' ' + last_name
     
            m10 = re.search(r"(.*?)\s+(.*)$", first_name)
            if m10: 
                # print("m10", itemdesc_orig)
                first_name = m10.group(1)
                middle_initial = m10.group(2)
        
    if name:
        # print("name", itemdesc_orig)
        try:
          return json.dumps({'name' : name,
                'first_name' : first_name, 
                'middle_initial' : middle_initial, 
                'last_name' : last_name, 
                'suffix' : suffix, 
                'prefix' :prefix})
        except TypeError:
          print(itemdesc_orig)        
    else:
        # print('No go!')
        return json.dumps({'name' : None, 
                'first_name' : None, 
                'middle_initial' : None, 
                'last_name' : None, 
                'suffix' : None, 
                'prefix' : None})
extract_name('Elect Director Jeffrey M. Leiden M.D.,Ph.D.')

temp = df.copy()[['itemdesc']]
temp['clean_name'] = temp['itemdesc'].map(extract_name)
temp[['itemdesc', 'clean_name']][:10] 

def expand_json(df, col):
    return pd.concat([df.drop([col], axis=1),
                      df[col].map(lambda x: json.loads(x)).apply(pd.Series)], axis=1)

df = expand_json(temp, 'clean_name')
df[ df['first_name'] == 'Ian'][['itemdesc', 'first_name', 'last_name']]

res = df.to_sql("director_names", engine, schema="risk", 
                if_exists="replace", index=False)

def process_sql(sql, engine):

    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(text(sql))
        trans.commit()
    except:
        trans.rollback()
        raise

    return True

process_sql("ALTER TABLE risk.director_names OWNER TO risk", engine)
process_sql("GRANT SELECT ON risk.director_names TO risk_access", engine)

engine.dispose()

