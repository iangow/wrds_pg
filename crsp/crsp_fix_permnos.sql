DROP VIEW activist_director.permnos;

CREATE OR REPLACE VIEW activist_director.permnos AS 
    SELECT DISTINCT stocknames.permno,
    stocknames.ncusip
    FROM crsp.stocknames
    WHERE stocknames.ncusip IS NOT NULL
    UNION
    SELECT DISTINCT missing_permnos.permno,
    missing_permnos.cusip AS ncusip
    FROM activist_director.missing_permnos
    WHERE missing_permnos.permno IS NOT NULL;

ALTER VIEW activist_director.permnos OWNER TO activism;
