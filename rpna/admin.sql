-- In R, system("psql -f admin.sql") runs this
CREATE SCHEMA rpna;
CREATE ROLE rpna;
CREATE ROLE rpna_access;
ALTER SCHEMA rpna OWNER TO rpna;
GRANT USAGE ON SCHEMA rpna TO rpna_access;
