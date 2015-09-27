-- Create a table that integrates basic returns with delisting returns
SET work_mem='2GB';

DROP TABLE IF EXISTS crsp.rets;

CREATE TABLE crsp.rets AS
    WITH
    dsedelist AS (
        SELECT permno, dlstdt AS date, dlret
        FROM crsp.dsedelist),

    dsf_plus AS (
        SELECT permno, date,
            coalesce(1+a.ret,1) * coalesce(1+b.dlret,1)-1 AS ret
        FROM crsp.dsf AS a
        FULL OUTER JOIN dsedelist AS b
        USING (permno, date)
        WHERE a.ret IS NOT NULL OR b.dlret IS NOT NULL),

    dsf_w_erdport AS (
        SELECT a.*, b.decret
        FROM dsf_plus AS a
        LEFT JOIN crsp.erdport1 AS b
        USING (permno, date))

    SELECT c.*, d.vwretd
    FROM dsf_w_erdport AS c
    LEFT JOIN crsp.dsi AS d
    USING (date);

ANALYZE crsp.rets;

RESET work_mem;

-- Create indexes on PERMNO and DATE
SET maintenance_work_mem='5GB';
CREATE INDEX ON crsp.rets (permno);
CREATE INDEX ON crsp.rets (date);
