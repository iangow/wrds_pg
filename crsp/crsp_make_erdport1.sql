DROP VIEW IF EXISTS crsp.erdport1;

CREATE VIEW crsp.erdport1 AS
WITH c AS (
	SELECT a.permno, a.ret, a.date, b.capn
	FROM crsp.dsf AS a
	INNER JOIN crsp.dport1 AS b
	ON a.permno=b.permno AND extract (year FROM a.date)=b.year)
SELECT c.*, d.decret
FROM c
INNER JOIN crsp.erdport AS d
ON c.date=d.date AND c.capn=d.capn;

ALTER VIEW crsp.erdport1 OWNER TO crsp;

GRANT SELECT ON crsp.erdport1 TO crsp_access;
