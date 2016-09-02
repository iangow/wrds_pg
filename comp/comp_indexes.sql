SET maintenance_work_mem='10GB';

CREATE INDEX ON comp.anncomp (gvkey);
CREATE INDEX ON comp.adsprate (gvkey, datadate);
CREATE INDEX ON comp.co_hgic (gvkey);
CREATE INDEX ON comp.co_ifndq (gvkey, datadate);
CREATE INDEX ON comp.company (gvkey);
CREATE INDEX ON comp.funda (gvkey, datadate);
CREATE INDEX ON comp.fundq (gvkey, datadate);
CREATE INDEX ON comp.g_sec_divid (gvkey, datadate);
CREATE INDEX ON comp.idx_ann (datadate);
CREATE INDEX ON comp.io_qbuysell (gvkey, datadate);
CREATE INDEX ON comp.names (gvkey);
CREATE INDEX ON comp.secm (gvkey, datadate);
CREATE INDEX ON comp.secd (gvkey, datadate);
CREATE INDEX ON comp.spind_mth (gvkey, datadate);
CREATE INDEX ON comp.wrds_segmerged (gvkey, datadate);

