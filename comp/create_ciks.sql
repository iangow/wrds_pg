DROP TABLE IF EXISTS comp.ciks;

CREATE TABLE comp.ciks AS
    SELECT cusip, cik
    FROM (SELECT DISTINCT gvkey, cusip FROM comp.secm) AS a
    INNER JOIN (SELECT DISTINCT gvkey, cik FROM comp.company) AS b
    USING (gvkey);

ALTER TABLE comp.ciks OWNER TO comp;
GRANT ALL ON TABLE comp.ciks TO comp;
GRANT SELECT ON TABLE comp.ciks TO comp_access;

SET timezone TO 'UTC';

DO
$do$
BEGIN
EXECUTE 'COMMENT ON TABLE comp.ciks IS ''CREATED USING comp/create_ciks.sql ON '
     || to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS TZ')
     || '''';
END
$do$
