-- Create a table that integrates basic returns with delisting returns
SET work_mem='6GB';

CREATE OR REPLACE FUNCTION eomonth(date)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
    SELECT (date_trunc('MONTH', $1) + INTERVAL '1 month - 1 day')::date;
$function$;

CREATE OR REPLACE FUNCTION eomonth(timestamp without time zone)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
    SELECT (date_trunc('MONTH', $1) + INTERVAL '1 month - 1 day')::date;
  $function$;

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

-- Create an index/key on PERMNO, DATE
RESET work_mem;

SET maintenance_work_mem='6GB';
CREATE INDEX ON crsp.mrets (permno, date);

ANALYZE crsp.mrets;

