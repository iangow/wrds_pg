SET maintenance_work_mem='3GB';

CREATE INDEX ON ciq.wrds_keydev (companyid);
CREATE INDEX ON ciq.wrds_keydev (keydeveventtypeid);
CREATE INDEX ON ciq.wrds_gvkey  (companyid);
CREATE INDEX ON ciq.wrds_cusip (companyid);
CREATE INDEX ON ciq.wrds_cik (companyid);
