ALTER TABLE audit.diroffichange ALTER COLUMN is_bdmem_pers TYPE integer;
ALTER TABLE audit.diroffichange ALTER COLUMN is_bdmem_pers TYPE boolean USING (is_bdmem_pers=1);
ALTER TABLE audit.diroffichange ALTER COLUMN is_ceo TYPE integer;
ALTER TABLE audit.diroffichange ALTER COLUMN is_ceo TYPE boolean USING (is_ceo=1);
ALTER TABLE audit.diroffichange ALTER COLUMN do_pers_co_key TYPE integer;

SET maintenance_work_mem='2GB';
CREATE INDEX ON audit.diroffichange (do_pers_co_key);
