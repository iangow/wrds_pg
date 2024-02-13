-- Create a table that integrates basic returns with delisting returns
DROP TABLE IF EXISTS crsp.mrets CASCADE;

CREATE TABLE crsp.mrets AS
    WITH
    msf_plus AS (
        SELECT
            coalesce(a.permno,b.permno) as permno,
            coalesce(a.date,b.dlstdt) as date,
            coalesce(1+a.ret,1) * coalesce(1+b.dlret,1)-1 AS ret
        FROM crsp.msf AS a
        FULL OUTER JOIN crsp.msedelist AS b
        ON a.permno=b.permno and a.date = b.dlstdt
        WHERE a.ret IS NOT NULL OR b.dlret IS NOT NULL),

    msf_w_ermport AS (
        SELECT a.*, b.decret
        FROM msf_plus AS a
        LEFT JOIN crsp.ermport1 AS b
        ON a.permno=b.permno AND a.date=b.date)

    SELECT c.*, d.vwretd
    FROM msf_w_ermport AS c
    LEFT JOIN crsp.msi AS d
    ON eomonth(c.date)=eomonth(d.date);

ALTER TABLE crsp.mrets OWNER TO crsp;

GRANT SELECT ON TABLE crsp.mrets TO crsp_access;

-- Create an index/key on PERMNO, DATE
CREATE INDEX ON crsp.mrets (permno, date);


