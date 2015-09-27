ALTER TABLE audit.feed14case ADD COLUMN case_start_date date;
ALTER TABLE audit.feed14case ADD COLUMN case_end_date date;
ALTER TABLE audit.feed14case ADD COLUMN exp_start_date date;
ALTER TABLE audit.feed14case ADD COLUMN exp_end_date date;

ALTER TABLE audit.feed14case ALTER COLUMN legal_case_key TYPE integer;
ALTER TABLE audit.feed14case ALTER COLUMN law_court_key TYPE integer;
ALTER TABLE audit.feed14case ALTER COLUMN judge_key TYPE integer;
ALTER TABLE audit.feed14case ALTER COLUMN der_legal_case_fkey TYPE integer;
ALTER TABLE audit.feed14case ALTER COLUMN lcd_ref_id TYPE integer;

ALTER TABLE audit.feed14case ADD COLUMN create_date_temp date;
UPDATE audit.feed14case SET create_date_temp=create_date::date;
ALTER TABLE audit.feed14case DROP COLUMN create_date;
ALTER TABLE audit.feed14case RENAME COLUMN create_date_temp TO create_date;

ALTER TABLE audit.feed14case ADD COLUMN change_date_temp date;
UPDATE audit.feed14case SET change_date_temp=change_date::date;
ALTER TABLE audit.feed14case DROP COLUMN change_date;
ALTER TABLE audit.feed14case RENAME COLUMN change_date_temp TO change_date;

ALTER TABLE audit.feed14case DROP COLUMN case_start_date_s; 
ALTER TABLE audit.feed14case DROP COLUMN case_end_date_s;
ALTER TABLE audit.feed14case DROP COLUMN exp_start_date_s;
ALTER TABLE audit.feed14case DROP COLUMN  exp_end_date_s;