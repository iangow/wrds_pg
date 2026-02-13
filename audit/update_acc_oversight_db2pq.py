#!/usr/bin/env python3
"""Update selected Audit Analytics tables via db2pq.wrds_update_pg().

This is a db2pq-adapted copy of wrds_pg/audit/update_acc_oversight.py.
"""

from db2pq import wrds_update_pg
from db2pq.postgres.comments import get_wrds_conn
from db2pq.postgres.introspect import get_table_columns


def resolve_drop_columns(
    table_name,
    schema,
    *,
    wrds_schema=None,
    wrds_id=None,
    prefix_drop=("match", "prior", "closest"),
    explicit_drop=None,
):
    """Build a db2pq-compatible drop list, including prefix-based column drops."""
    source_schema = wrds_schema or schema
    explicit_drop = explicit_drop or []

    with get_wrds_conn(wrds_id) as wrds:
        cols = get_table_columns(wrds, source_schema, table_name)

    drop = set(explicit_drop)
    for c in cols:
        if any(c.startswith(prefix) for prefix in prefix_drop):
            drop.add(c)

    return sorted(drop)


def update_table(
    table_name,
    *,
    schema="audit",
    wrds_schema=None,
    wrds_id=None,
    col_types=None,
    explicit_drop=None,
):
    drop = resolve_drop_columns(
        table_name=table_name,
        schema=schema,
        wrds_schema=wrds_schema,
        wrds_id=wrds_id,
        explicit_drop=explicit_drop,
    )

    return wrds_update_pg(
        table_name=table_name,
        schema=schema,
        wrds_schema=wrds_schema,
        wrds_id=wrds_id,
        drop=drop or None,
        col_types=col_types,
    )


if __name__ == "__main__":
    updated = update_table(
        "feed55_auditor_ratification",
        col_types={
            "auditor_ratification_fkey": "integer",
            "share_class_fkey": "integer",
            "auditor_fkey": "integer",
            "pcaob_registration_number": "integer",
        },
    )

    update_table(
        "feed56_accounting_estimates_chan",
        col_types={"accounting_estimates_cha_fke": "integer"},
    )

    update_table(
        "feed65_impairments",
        explicit_drop=[
            "material_impairment_text",
            "mtrial_impairment_text_html",
        ],
        col_types={
            "mtrl_imprmnt_fct_key": "integer",
            "mtrl_imprmnt_key": "integer",
            "quantitative_taxonomy_fkey": "integer",
            "eventdate_aud_fkey": "integer",
            "is_range": "boolean",
            "is_final": "boolean",
        },
    )

    # update_table(
    #     "feed74_aqrm",
    #     col_types={
    #         "flag_year": "integer",
    #         "fye_of_opinion": "integer",
    #         "ideal_fye_of_opinion": "integer",
    #     },
    # )

    update_table(
        "feed78_critical_audit_matters",
        col_types={
            "audit_opinion_fkey": "integer",
            "auditor_fkey": "integer",
            "critical_audit_matter_key": "integer",
            "critical_audit_matter_topic_fkey": "integer",
        },
    )

    # update_table(
    #     "feed85_cybersecurity",
    #     col_types={
    #         "cybersecurity_breach_key": "integer",
    #         "number_of_records_lost": "integer",
    #     },
    # )

    # update_table("feed86_audit_firm_events")

    update_table(
        "feed89_pcaob_report",
        col_types={
            "auditor_report_key": "integer",
            "auditor_affiliate_fkey": "integer",
            "inspection_year": "integer",
            "auditor_fkey": "integer",
            "has_firm_signed": "boolean",
            "has_written_response": "boolean",
            "is_clean_report": "boolean",
        },
    )

    update_table(
        "feed91_aaer",
        col_types={
            "aaer_event_key": "integer",
            "first_release_fkey": "integer",
            "most_recent_release_fkey": "integer",
        },
    )
