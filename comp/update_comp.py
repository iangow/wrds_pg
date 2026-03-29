#!/usr/bin/env python3

from db2pq import process_sql, wrds_update_pg

# ----------------------------
# Helpers
# ----------------------------
def update(table: str, schema: str = "comp", **kwargs) -> bool:
    """Run wrds_update_pg and return whether an update occurred."""
    return wrds_update_pg(table, schema, **kwargs)


def index_if_updated(updated: bool, sql: str) -> None:
    """Create index only if updated is True."""
    if updated:
        process_sql(sql)


def create_index(schema: str, table: str, cols: str) -> str:
    """Return a simple CREATE INDEX statement."""
    return f'CREATE INDEX ON "{schema}"."{table}" ({cols})'


# ----------------------------
# Main
# ----------------------------
def main() -> None:

    # ----------------------------------------
    # "Core" comp tables (non-global)
    # ----------------------------------------
    update("sec_history")
    update("idxcst_his")  # appears multiple times in your script; keeping first

    updated = update("anncomp")
    index_if_updated(updated, create_index("comp", "anncomp", "gvkey"))

    updated = update("adsprate")
    index_if_updated(updated, create_index("comp", "adsprate", "gvkey, datadate"))

    updated = update("co_afnd2")
    index_if_updated(updated, create_index("comp", "co_afnd2", "gvkey"))

    updated = update("co_hgic")
    index_if_updated(updated, create_index("comp", "co_hgic", "gvkey"))

    updated = update("co_ifndq")
    index_if_updated(updated, create_index("comp", "co_ifndq", "gvkey, datadate"))

    company_updated = update("company")
    index_if_updated(company_updated, create_index("comp", "company", "gvkey"))

    updated = update("idx_ann")
    index_if_updated(updated, create_index("comp", "idx_ann", "datadate"))

    update("co_filedate")

    updated = update("secd", col_types = {"cshoc": "float8"})
    index_if_updated(updated, create_index("comp", "secd", "gvkey, datadate"))

    updated = update("idx_index")

    update("idx_daily")

    updated = update("io_qbuysell")
    index_if_updated(updated, create_index("comp", "io_qbuysell", "gvkey, datadate"))

    updated = update("names")
    index_if_updated(updated, create_index("comp", "names", "gvkey"))

    update("names_ix")

    secm_updated = update("secm", col_types={"cshom": "float8"})
    index_if_updated(secm_updated, create_index("comp", "secm", "gvkey, datadate"))

    updated = update("spind_mth")
    index_if_updated(updated, create_index("comp", "spind_mth", "gvkey, datadate"))

    updated = update("funda")
    index_if_updated(updated, create_index("comp", "funda", "gvkey, datadate"))

    updated = update("fundq")
    index_if_updated(updated, create_index("comp", "fundq", "gvkey, datadate"))

    updated = update("funda_fncd")
    index_if_updated(updated, create_index("comp", "funda_fncd", "gvkey, datadate"))

    updated = update("fundq_fncd")
    index_if_updated(updated, create_index("comp", "fundq_fncd", "gvkey, datadate"))

    updated = update("aco_pnfnda")
    index_if_updated(updated, create_index("comp", "aco_pnfnda", "gvkey, datadate"))

    updated = update("sec_divid")
    index_if_updated(updated, create_index("comp", "sec_divid", "gvkey, datadate"))

    update("r_giccd")
    update("r_auditors")
    update("r_datacode")
    update("r_ex_codes")
    update("security")

    # ----------------------------------------
    # Global ("g_*") tables
    # ----------------------------------------
    update("g_exrt_dly")

    updated = update("g_secm")
    index_if_updated(updated, create_index("comp", "g_secm", "gvkey"))

    updated = update("g_secd", col_types = {"cshoc": "float8"})
    index_if_updated(updated, create_index("comp", "g_secd", "gvkey"))

    updated = update("g_funda")
    index_if_updated(updated, create_index("comp", "g_funda", "gvkey, datadate"))

    update("g_security")
    update("g_company")

    updated = update("g_sec_divid")
    index_if_updated(updated, create_index("comp", "g_sec_divid", "gvkey, datadate"))

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
                     "gvkey, datadate"))

    # ----------------------------------------
    # Footnotes
    # ----------------------------------------
    update("r_fndfntcd")
    update("co_adesind")


if __name__ == "__main__":
    main()
