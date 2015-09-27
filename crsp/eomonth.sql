
CREATE OR REPLACE FUNCTION eomonth(date)
  RETURNS date AS
$BODY$
    SELECT (date_trunc('MONTH', $1) + INTERVAL '1 month - 1 day')::date;
  $BODY$
  LANGUAGE sql IMMUTABLE STRICT
  COST 100;

CREATE OR REPLACE FUNCTION eomonth(timestamp without time zone)
  RETURNS date AS
$BODY$
    SELECT (date_trunc('MONTH', $1) + INTERVAL '1 month - 1 day')::date;
  $BODY$
  LANGUAGE sql IMMUTABLE STRICT
  COST 100;
