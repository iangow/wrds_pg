-- In R, system("psql -f admin.sql") runs this
CREATE SCHEMA prna;
CREATE ROLE prna;
CREATE ROLE prna_access;
ALTER SCHEMA prna OWNER TO prna;
GRANT USAGE ON SCHEMA prna TO prna_access;
