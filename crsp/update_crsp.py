#!/usr/bin/env python3
from datetime import datetime, timezone

from wrds2pg import (
    make_engine, process_sql, run_file_sql, 
    set_table_comment, wrds_update
)

def utc_timestamp() -> str:
    """Return an ISO-ish UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def comment_created_by(script_name: str) -> str:
    return f"Created using {script_name} on {utc_timestamp()} (UTC)."


def create_indexes(engine, statements: list[str]) -> None:
    for stmt in statements:
        process_sql(stmt, engine)


def main() -> None:
    script_name = "update_crsp.py"
    engine = make_engine()

    # --- Treasury yield table crsp.tfz_ft ---
    
    # From WRDS:
    # The error is correct, the table "tfz_ft," does not exist. Behind the scenes this web
    # query form is joining two tables on the fly. The tables this query is joining are
    # "crsp.tfz_idx" and either "crsp.tfz_dly_ft" or "crsp.tfz_mth_ft" (depending on if
    # you want daily or monthly data) by the variable "kytreasnox."
    
    # Here are some links to the information about these tables:
    # https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77140
    # https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77137
    # https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77147
    # Note: WRDS's "tfz_ft" web query joins "crsp.tfz_idx" with "crsp.tfz_dly_ft"
    # (daily) or "crsp.tfz_mth_ft" (monthly) on "kytreasnox".
    tfz_idx = wrds_update(
        "tfz_idx",
        "crsp",
        col_types={"kytreasnox": "integer", "ttermtype": "integer", "rdtreasno": "integer"},
    )
    tfz_dly_ft = wrds_update(
        "tfz_dly_ft",
        "crsp",
        col_types={
            "kytreasnox": "integer",
            "tdyearstm": "float8",
            "tdduratn": "float8",
            "tdytm": "float8",
            "tdbid": "float8",
            "tdask": "float8",
            "tdnomprc": "float8",
            "tdaccint": "float8",
            "tdretadj": "float8",
        },
    )

    if tfz_idx or tfz_dly_ft:
        sql = """
        DROP TABLE IF EXISTS crsp.tfz_ft;
        CREATE TABLE crsp.tfz_ft AS
        SELECT
            kytreasnox, tidxfam, ttermtype, ttermlbl, caldt, rdtreasno, rdcrspid
        FROM crsp.tfz_idx
        INNER JOIN crsp.tfz_dly_ft
        USING (kytreasnox);
        """
        process_sql(sql, engine)
        process_sql("ALTER TABLE crsp.tfz_ft OWNER TO crsp", engine)
        process_sql("GRANT SELECT ON crsp.tfz_ft TO crsp_access", engine)
        set_table_comment("tfz_ft", "crsp", comment_created_by(script_name), engine)

    # --- Monthly data ---
    wrds_update("mse", "crsp", fix_missing=True)

    msf = wrds_update(
        "msf",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "permco": "integer"},
    )
    if msf:
        create_indexes(
            engine,
            [
                "CREATE INDEX ON crsp.msf (date)",
                "CREATE INDEX ON crsp.msf (permno, date)",
                "CREATE INDEX ON crsp.msf (permno)",
                "CREATE INDEX ON crsp.msf (permco)",
            ],
        )

    msi = wrds_update("msi", "crsp")
    if msi:
        create_indexes(engine, ["CREATE INDEX ON crsp.msi (date)"])

    msedelist = wrds_update("msedelist", "crsp", fix_missing=True)

    ermport1 = wrds_update(
        "ermport1",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "capn": "integer"},
    )

    # --- Daily data ---
    dsf = wrds_update(
        "dsf",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "permco": "integer"},
    )
    if dsf:
        process_sql("SET maintenance_work_mem='1999MB'", engine)
        create_indexes(
            engine,
            [
                "CREATE INDEX ON crsp.dsf (permno, date)",
                "CREATE INDEX ON crsp.dsf (permco)",
                "CREATE INDEX ON crsp.dsf (permno)",
            ],
        )

    wrds_update("dsi", "crsp")

    dsedelist = wrds_update(
        "dsedelist",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "permco": "integer"},
    )
    if dsedelist:
        create_indexes(engine, ["CREATE INDEX ON crsp.dsedelist (permno)"])

    erdport1 = wrds_update(
        "erdport1",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "capn": "integer"},
    )
    if erdport1:
        create_indexes(engine, ["CREATE INDEX ON crsp.erdport1 (permno, date)"])

    ccmxpf_linktable = wrds_update(
        "ccmxpf_linktable",
        "crsp",
        fix_missing=True,
        col_types={"lpermno": "integer",
                   "lpermco": "integer", 
                   "usedflag": "integer"},
    )
    if ccmxpf_linktable:
        create_indexes(
            engine,
            [
                "CREATE INDEX ON crsp.ccmxpf_linktable (lpermno)",
                "CREATE INDEX ON crsp.ccmxpf_linktable (gvkey)",
            ],
        )

    ccmxpf_lnkhist = wrds_update(
        "ccmxpf_lnkhist",
        "crsp",
        fix_missing=True,
        col_types={"lpermno": "integer", "lpermco": "integer"},
    )
    if ccmxpf_lnkhist:
        create_indexes(
            engine,
            [
                "CREATE INDEX ON crsp.ccmxpf_lnkhist (gvkey)",
                "CREATE INDEX ON crsp.ccmxpf_lnkhist (lpermno)",
            ],
        )

    dsedist = wrds_update(
        "dsedist",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "permco": "integer"},
    )
    if dsedist:
        create_indexes(engine, ["CREATE INDEX ON crsp.dsedist (permno)"])

    dse = wrds_update(
        "dse",
        "crsp",
        fix_missing=True,
        col_types={"permno": "integer", "permco": "integer"},
    )
    if dse:
        create_indexes(engine, ["CREATE INDEX ON crsp.dse (permno)"])

    wrds_update("stocknames", "crsp",
                col_types={"permno": "integer",
                           "permco": "integer"})

    dseexchdates = wrds_update(
        "dseexchdates",
        "crsp",
        col_types={"permno": "integer", "permco": "integer"},
    )
    if dseexchdates:
        create_indexes(engine, ["CREATE INDEX ON crsp.dseexchdates (permno)"])

    # --- Other data sets ---
    wrds_update("msp500list", "crsp")
    wrds_update("ccmxpf_lnkused", "crsp", fix_missing=True)

    for tbl in [
        "dsp500",
        "dsp500p",
        "msp500",
        "msp500p",
        "mcti",
        "mcti_corr",
        "msedist",
        "mseshares",
    ]:
        wrds_update(tbl, "crsp")

    wrds_update("comphist", "crsp", fix_missing=True)

    engine.dispose()


if __name__ == "__main__":
    main()
