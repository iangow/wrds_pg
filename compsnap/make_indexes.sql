SET maintenance_work_mem='3GB';
CREATE INDEX ON compsnap.wrds_csq_pit (pitdate1, pitdate2);
CREATE INDEX ON compsnap.wrds_csq_pit (gvkey);
ALTER TABLE compsnap.wrds_csq_pit 
    ADD PRIMARY KEY (gvkey, pitdate1, pitdate2, datadate,
                     indfmt, datafmt, consol, popsrc);
