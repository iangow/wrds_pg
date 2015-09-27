ALTER TABLE audit.bankrupt ALTER COLUMN bank_key TYPE integer;
ALTER TABLE audit.bankrupt ALTER COLUMN bankruptcy_type TYPE integer;
ALTER TABLE audit.bankrupt ALTER COLUMN law_court_fkey TYPE integer;
ALTER TABLE audit.bankrupt ALTER COLUMN court_type_code TYPE integer USING court_type_code::integer ;
ALTER TABLE audit.bankrupt ALTER COLUMN eventdate_aud_fkey TYPE integer;

ALTER TABLE audit.bankrupt ADD COLUMN subsid_fkey_temp integer;
UPDATE audit.bankrupt SET subsid_fkey_temp=
    CASE WHEN company_fkey='.' THEN NULL 
        ELSE subsid_fkey::integer END;
ALTER TABLE audit.bankrupt DROP COLUMN subsid_fkey;
ALTER TABLE audit.bankrupt RENAME COLUMN subsid_fkey_temp TO subsid_fkey;