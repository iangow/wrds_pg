ALTER TABLE audit.feed17person ALTER COLUMN do_person_at_company_key TYPE integer;
ALTER TABLE audit.feed17person ALTER COLUMN cd_personid_fkey TYPE integer;

SET maintenance_work_mem='2GB';
CREATE INDEX ON audit.feed17person (do_person_at_company_key);