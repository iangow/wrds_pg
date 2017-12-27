#!/usr/bin/env python3
from sqlalchemy import create_engine
import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
wrds_id = os.getenv("WRDS_ID")
engine = create_engine("postgresql://" + host + "/" + dbname)

# engine.execute("CREATE ROLE risk")
engine.execute("GRANT USAGE ON SCHEMA risk TO risk")

from wrds_fetch import wrds_update

update = wrds_update("vavoteresults", "risk", engine, wrds_id)

if update:
    sql = """
        ALTER TABLE risk.vavoteresults ALTER COLUMN companyid TYPE integer;
        ALTER TABLE risk.vavoteresults ALTER COLUMN meetingid TYPE integer;
        ALTER TABLE risk.vavoteresults ALTER COLUMN itemonagendaid TYPE integer;
        ALTER TABLE risk.vavoteresults ALTER COLUMN seqnumber TYPE integer;

        UPDATE risk.vavoteresults
            SET voterequirement = 0.6667 WHERE voterequirement=66.67;

        UPDATE risk.vavoteresults SET base = 'F+A' WHERE base='F A';

        UPDATE risk.vavoteresults SET base = 'F+A+AB'
            WHERE base IN ('F A AB', 'F+A+B');

        UPDATE risk.vavoteresults
            SET (votedfor, votedagainst, votedabstain)=(2224433656, 93561790, 34814753)
            WHERE itemonagendaid=6019529;

        UPDATE risk.vavoteresults
            SET (votedfor, votedagainst, votedabstain, ticker)= (10540862,1329889,790539,'KIDE')
            WHERE itemonagendaid=6039421;

        UPDATE risk.vavoteresults
            SET voteresult='Pass' WHERE itemonagendaid=7495200;

        UPDATE risk.vavoteresults
            SET (votedfor, voteresult)=(14830551, 'Pass')
            WHERE itemonagendaid=6049938;

        -- Source: ../912463/000110465904022325/a04-8530_110q.htm
        UPDATE risk.vavoteresults
            SET (votedfor, voteresult)=(30136926, 'Pass')
            WHERE itemonagendaid=6251118;

        --  Source: ../1050606/000105060603000027/form10q6302003.htm
        UPDATE risk.vavoteresults
            SET (votedfor, voteresult)=(70548942, 'Pass')
            WHERE itemonagendaid=6049746;
    """
    engine.execute(sql)

wrds_update("issrec", "risk", engine, wrds_id)
wrds_update("globalvoteresults", "risk", engine, wrds_id)
wrds_update("proposals", "risk", engine, wrds_id)
