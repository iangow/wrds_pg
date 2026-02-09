#!/usr/bin/env python3

from wrds2pg import make_engine, process_sql, wrds_update

# ----------------------------
# Helpers
# ----------------------------
def update(table: str, schema: str = "comp", **kwargs) -> bool:
    """Run wrds_update and return whether an update occurred."""
    return wrds_update(table, schema, **kwargs)


def index_if_updated(updated: bool, sql: str, *, engine) -> None:
    """Create index only if updated is True."""
    if updated:
        process_sql(sql, engine=engine)


def create_index(schema: str, table: str, cols: str) -> str:
    """Return a simple CREATE INDEX statement."""
    return f'CREATE INDEX ON "{schema}"."{table}" ({cols})'


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    engine = make_engine()

    # ----------------------------------------
    # "Core" comp tables (non-global)
    # ----------------------------------------
    update("sec_history")
    update("idxcst_his")  # appears multiple times in your script; keeping first

    updated = update("anncomp")
    index_if_updated(updated, create_index("comp", "anncomp", "gvkey"), engine=engine)

    updated = update("adsprate")
    index_if_updated(updated, create_index("comp", "adsprate", "gvkey, datadate"), engine=engine)

    updated = update("co_afnd2")
    index_if_updated(updated, create_index("comp", "co_afnd2", "gvkey"), engine=engine)

    updated = update("co_hgic")
    index_if_updated(updated, create_index("comp", "co_hgic", "gvkey"), engine=engine)

    updated = update("co_ifndq")
    index_if_updated(updated, create_index("comp", "co_ifndq", "gvkey, datadate"), engine=engine)

    company_updated = update("company")
    index_if_updated(company_updated, create_index("comp", "company", "gvkey"), engine=engine)

    updated = update("idx_ann")
    index_if_updated(updated, create_index("comp", "idx_ann", "datadate"), engine=engine)

    update("co_filedate")

    updated = update("secd", col_types = {"cshoc": "float8"})
    index_if_updated(updated, create_index("comp", "secd", "gvkey, datadate"), engine=engine)

    updated = update("idx_index")

    updated = update("io_qbuysell")
    index_if_updated(updated, create_index("comp", "io_qbuysell", "gvkey, datadate"), engine=engine)

    updated = update("names")
    index_if_updated(updated, create_index("comp", "names", "gvkey"), engine=engine)

    secm_updated = update("secm", col_types={"cshom": "float8"})
    index_if_updated(secm_updated, create_index("comp", "secm", "gvkey, datadate"), engine=engine)

    updated = update("spind_mth")
    index_if_updated(updated, create_index("comp", "spind_mth", "gvkey, datadate"), engine=engine)

    updated = update("funda", fix_missing=True)
    index_if_updated(updated, create_index("comp", "funda", "gvkey, datadate"), engine=engine)

    updated = update("fundq", fix_missing=True)
    index_if_updated(updated, create_index("comp", "fundq", "gvkey, datadate"), engine=engine)

    updated = update("funda_fncd")
    index_if_updated(updated, create_index("comp", "funda_fncd", "gvkey, datadate"), engine=engine)

    updated = update("fundq_fncd")
    index_if_updated(updated, create_index("comp", "fundq_fncd", "gvkey, datadate"), engine=engine)

    updated = update("aco_pnfnda")
    index_if_updated(updated, create_index("comp", "aco_pnfnda", "gvkey, datadate"), engine=engine)

    updated = update("sec_divid", fix_missing=True)
    index_if_updated(updated, create_index("comp", "sec_divid", "gvkey, datadate"), engine=engine)

    update("r_giccd")
    update("r_auditors")
    update("r_datacode")

    # ----------------------------------------
    # Global ("g_*") tables
    # ----------------------------------------
    update("g_exrt_dly")

    updated = update("g_secm")
    index_if_updated(updated, create_index("comp", "g_secm", "gvkey"), engine=engine)

    updated = update("g_secd")
    index_if_updated(updated, create_index("comp", "g_secd", "gvkey"), engine=engine)

    updated = update("g_funda", fix_missing=True)
    index_if_updated(updated, create_index("comp", "g_funda", "gvkey, datadate"), engine=engine)

    update("g_security")
    update("g_company")

    updated = update("g_sec_divid", fix_missing=True)
    index_if_updated(updated, create_index("comp", "g_sec_divid", "gvkey, datadate"), engine=engine)

    update("g_idxcst_his")
    update("g_idx_index")
    update("g_idx_mth")
    update("g_secnamesd")
    update("g_names_ix")
    update("g_names_ix_cst")
    update("g_names")
    update("g_namesq")
    update("g_chars")

    # ----------------------------------------
    # Segment tables
    # ----------------------------------------
    update("seg_annfund")
    update("seg_customer")
    update("wrds_seg_customer")

    updated = update("wrds_segmerged")
    index_if_updated(updated, 
                     create_index("comp", "wrds_segmerged",
                     "gvkey, datadate"), engine=engine)

    # ----------------------------------------
    # Footnotes
    # ----------------------------------------
    update("r_fndfntcd")
    update("co_adesind")

    engine.dispose()


if __name__ == "__main__":
    main()
