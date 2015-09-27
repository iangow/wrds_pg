DROP VIEW IF EXISTS crsp.ermport1;

CREATE VIEW crsp.ermport1 AS
SELECT c.*, d.decret FROM 
	(SELECT a.permno, a.ret, a.date, b.capn
	FROM crsp.msf AS a
	INNER JOIN crsp.mport1 AS b
	ON a.permno=b.permno AND extract (year FROM a.date)=b.year) AS c
INNER JOIN crsp.ermport AS d
ON c.date=d.date AND c.capn=d.capn;
