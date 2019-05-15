DROP TABLE IF EXISTS comp.ciks;

CREATE TABLE comp.ciks AS 
    SELECT cusip, cik 
    FROM (SELECT DISTINCT gvkey, cusip FROM comp.secm) AS a
    INNER JOIN (SELECT DISTINCT gvkey, cik FROM comp.company) AS b
    USING (gvkey);

ALTER TABLE comp.ciks OWNER TO comp;
GRANT ALL ON TABLE comp.ciks TO comp;
GRANT SELECT ON TABLE comp.ciks TO comp_access;