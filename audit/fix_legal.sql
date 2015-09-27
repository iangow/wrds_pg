

ALTER TABLE audit.feed14party ADD COLUMN company_fkey_temp  integer;

UPDATE audit.feed14party SET company_fkey_temp=CASE WHEN company_fkey='.' THEN NULL ELSE company_fkey::integer END;
ALTER TABLE audit.feed14party DROP COLUMN company_fkey;
ALTER TABLE audit.feed14party RENAME COLUMN company_fkey_temp TO company_fkey;

ALTER TABLE audit.feed14party ALTER COLUMN auditor_key TYPE integer;
ALTER TABLE audit.feed14party ALTER COLUMN gov_key TYPE integer;
ALTER TABLE audit.feed14party ALTER COLUMN law_firm_key TYPE integer;
ALTER TABLE audit.feed14party ALTER COLUMN legal_case_key TYPE integer;

ALTER TABLE audit.feed14party ALTER COLUMN defendant TYPE boolean USING defendant=1;
ALTER TABLE audit.feed14party ALTER COLUMN plaintiff TYPE boolean USING plaintiff=1;
ALTER TABLE audit.feed14party ALTER COLUMN is_lead TYPE boolean USING is_lead=1;
ALTER TABLE audit.feed14party ALTER COLUMN consol TYPE boolean USING consol=1;
ALTER TABLE audit.feed14party ALTER COLUMN rel_non_party TYPE boolean USING rel_non_party=1;
ALTER TABLE audit.feed14party ALTER COLUMN rel_defendant TYPE boolean USING rel_defendant=1;
ALTER TABLE audit.feed14party ALTER COLUMN third_party TYPE boolean USING third_party=1;
ALTER TABLE audit.feed14party ALTER COLUMN is_debtor TYPE boolean USING is_debtor=1;
ALTER TABLE audit.feed14party ALTER COLUMN is_creditor TYPE boolean USING is_creditor=1;
ALTER TABLE audit.feed14party ALTER COLUMN been_terminated TYPE boolean USING been_terminated=1;



