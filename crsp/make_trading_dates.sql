-- SELECT CURRENT_DATE + s.a AS dates
--   FROM generate_series(0,14,7) as s(a);
-- DROP TABLE IF EXISTS crsp.trading_dates CASCADE;
DROP TABLE IF EXISTS crsp.trading_dates CASCADE;

CREATE TABLE crsp.trading_dates AS
SELECT date, (rank() OVER (ORDER BY date))::integer AS td
    FROM (SELECT DISTINCT DATE FROM crsp.dsi) AS a;

CREATE INDEX ON crsp.trading_dates (date);
CREATE INDEX trading_dates_td_idx ON crsp.trading_dates (td);
CLUSTER crsp.trading_dates USING trading_dates_td_idx;
ANALYZE crsp.trading_dates;
ALTER TABLE crsp.trading_dates OWNER TO crsp;
GRANT SELECT ON TABLE crsp.trading_dates TO crsp_access;

DROP VIEW IF EXISTS crsp.all_dates;

CREATE VIEW crsp.all_dates AS
SELECT anncdate::date
FROM generate_series((SELECT min(date) FROM crsp.trading_dates)::timestamp,
		(SELECT max(date) FROM crsp.trading_dates), '1 day') AS anncdate;
ALTER TABLE crsp.all_dates OWNER TO crsp;
GRANT SELECT ON TABLE crsp.all_dates TO crsp_access;

DROP TABLE IF EXISTS crsp.anncdates;

CREATE TABLE crsp.anncdates AS
SELECT a.anncdate, min(td) OVER w AS td, min(date) OVER w AS date
FROM crsp.all_dates AS a
LEFT JOIN crsp.trading_dates AS b
ON b.date = a.anncdate
WINDOW w AS (ORDER BY anncdate ROWS BETWEEN CURRENT ROW AND 7 FOLLOWING)
ORDER BY anncdate;

CREATE INDEX anncdates_anncdate_idx ON crsp.anncdates (anncdate);
CLUSTER crsp.anncdates USING anncdates_anncdate_idx;
ANALYZE crsp.anncdates;

CREATE INDEX ON crsp.anncdates (anncdate);
ALTER TABLE crsp.anncdates OWNER TO crsp;
GRANT SELECT ON TABLE crsp.anncdates TO crsp_access;
