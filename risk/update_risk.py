#!/usr/bin/env python3
from wrds2pg import wrds_update, make_engine

update = wrds_update("vavoteresults", "risk",
                     col_types = {'companyid':'integer', 
                                   'meetingid':'integer',
                                   'itemonagendaid':'integer',
                                   'seqnumber':'integer'})
if update:
    engine = make_engine()
    sql = """
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
    process_sql(sql, engine)

wrds_update("globalvoteresults", "risk")
wrds_update("gset", "risk")
wrds_update("votes", "risk")
wrds_update("rmgovernance", "risk", 
                      col_types={'company_id':'integer'})
wrds_update("directors", "risk",
                      col_types={'annrev': 'float8',
                                 'year_term_ends': 'float8',
                                 'voting': 'float8',
                                 'votecref':'float8',
                                 'outside_public_boards':'text'})
wrds_update("votes", "risk")
wrds_update("rmdirectors", "risk", 
                      col_types={'company_id':'integer',
                                 'pcnt_ctrl_votingpower':'float8'})
