# Audit Analytics


Data comes from the WRDS PostgreSQL `audit` schema and is imported here
using `db2pq`.

## Introduction

[Audit Analytics](https://www.auditanalytics.com/) “is an independent
research provider of audit, regulatory and disclosure intelligence.
Audit Analytics provides detailed data on over 150,000 active audits and
more than 10,000 accounting firms.” Please refer to the [Manuals and
Overviews](https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/audit-analytics/)
or download a [zip
file](https://github.com/iangow/wrds_pg/blob/master/audit/AuditAnalyticsManuals.zip?raw=true)
to view the data structures and variable definitions of Audit Analytics
via WRDS.

## List of datasets imported

Audit Analytics comprises five sets of data:

1.  Audit and Compliance
2.  Corporate and Legal
3.  Accounting and Oversight
4.  Other Independent Audit
5.  Canada (SEDAR)

At the University of Melbourne I had access only to the first three sets
of data.

### Audit and Compliance

This comprises 14 tables (table names listed in parentheses):

- Auditors (`feed01_auditors`)
- Auditor Changes (`feed02_auditor_changes`)
- Audit Fees (`feed03_audit_fees`)
- Audit Fees with Restatements (`feed04_audit_fees_restated`)
- Benefit Plan Opinions (`feed06_benefit_plan_opinions`)
- Audit Opinions (`feed05_audit_opinions`)
- Current Auditors (`feed07_current_auditor`)
- Non-Reliance Restatements (`feed09_nonreliance_restatements`)
- SOX 302 Disclosure Controls (`feed10_sox_302_disclosure_contro`)
- SOX 404 Internal Controls (`feed11_sox_404_internal_controls`)
- Accelerated Filer (`feed16_accelerated_filer`)
- Director and Officer Changes (`feed17_director_and_officer_chan`)
- Non-timely Filer Information And Analysis (`feed20_nt`)
- Revised Audit Opinions (`feed34_revised_audit_opinions`)

### Corporate and Legal

This comprises 12 tables (table names listed in parentheses):

- Legal Case (`feed13_legal_case_feed`)
- Legal Parties (`feed14_company_legal_party_feed`)
- Mergers and Acquisitions (`feed18_merger_acquisition`)
- IPO (`ipo`): To be renamed `feed19_ipo`?
- Bankruptcy Notification (`feed21_bankruptcy_notification`)
- Comment Letter (`commlett`): To be renamed `feed25_comment_letters`?
- Comment Letter Conversations (`feed26_comment_letter_conversati`)
- Shareholder Activism (`feed31_shareholder_activism`)
- Form D (`feed37_form_d`)
- Form D Most Recent Report (`feed38_form_d_most_recent_offeri`)
- Comment Threading (`feed40_comment_letter_threads`)
- Transfer Agents (`feed41_transfer_agents`)

In each case, the local database includes a table with the same name,
but in general, we do not include *all* variables provided in the WRDS
version of the table. A detailed discussion of the changes we make is
given in the next section.

## Issues identified and fixed

While Audit Analytics provides comprehensive data, the tables provided
by WRDS, unfortunately, are a complete mess and the data are often not
well-formatted. One particular issue is that the tables are very much
*not* normalized. So there is a lot of duplicated data and tables are
loaded with extraneous information. Also, some textual variables are
split across a number of columns, and some textual data is placed in
separate tables. We identified and fixed the following three issues.

### Omission of financial variables

Specifically, the WRDS tables often include variables from the “company
financial block”, which are financial statement variables such as net
income or total assets for the “`closest`”, “`match`”, “`hiwater`”, or
“`prior`” periods (either quarters \[`qu`\] or years \[`yr`\]). These
variables expand the size of the tables dramatically, and their
provenance is unclear (likely scraped from SEC filings, which are the
primary source for Audit Analytics data). Also, the meaning of the terms
“`closest`”, “`match`”, “`hiwater`”, and “`prior`” is unclear and the
data are very poorly documented on WRDS. So, we simply omit these
variables when importing the data.

### Textual variables

Several tables include fields with the text (or portions of the text) of
the underlying SEC filings. Some of these fields are still awkwardly
structured in WRDS, and there are historical reasons for odd naming and
layout in the local scripts, but the current workflow imports directly
from WRDS PostgreSQL rather than from WRDS SAS files.

At this point, the local PostgreSQL tables generally retain WRDS’s
PostgreSQL representation for these columns. In particular, this
repository no longer converts pipe-delimited text fields into PostgreSQL
arrays during import. The goal is to keep the imported tables close to
WRDS’s PostgreSQL structure while still omitting clearly redundant or
poorly documented variables.

### Omission of auditor names from some tables

The WRDS tables include auditor names as well as an auditor key (i.e.,
`auditor_key` as found on `auditorsinfo`). Because the `auditor_name`
appears on `auditors`, it is redundant to include the same information
on each table in which `auditor_key` appears. So we have deleted it from
many tables. Simply join the table with `auditors` using `auditor_key`
to recover this variable.

## Sample code

### Convert pipe-delimited text on the fly

Some Audit Analytics columns are easier to work with as arrays even
though they are stored in WRDS PostgreSQL as delimiter-separated text.
Rather than reshaping them at import time, you can do the conversion in
queries when needed.

For text values:

``` sql
SELECT
    t.comment_response_key,
    u.ord,
    u.question_issue_text,
    NULLIF(u.question_issue_key, '')::integer AS question_issue_key
FROM audit.feed40_comment_letter_threads AS t
CROSS JOIN LATERAL UNNEST(
    regexp_split_to_array(
        regexp_replace(t.question_issue_text_list, '(^\|+|\|+$)', '', 'g'),
        '\|+'
    ),
    regexp_split_to_array(
        regexp_replace(t.question_issue_key_list, '(^\|+|\|+$)', '', 'g'),
        '\|+'
    )
) WITH ORDINALITY AS u(question_issue_text, question_issue_key, ord);
```

| comment_response_key | ord | question_issue_text | question_issue_key |
|---:|---:|:---|---:|
| 498056 | 1 | EITF 00-21 issues | 664 |
| 498212 | 1 | Regulation S-X, Article 8 issues | 2832 |
| 498212 | 2 | Regulation S-X, Rule 8-02 issues | 2844 |
| 88516 | 1 | EITF 03-1 issues | 1025 |
| 88522 | 1 | FIN 46 (R) issues | 552 |
| 88522 | 2 | EITF 01-8 issues | 2863 |
| 88524 | 1 | SFAS 71 issues | 479 |
| 59425 | 1 | Regulation S-K, Item 101 issues | 763 |
| 59425 | 2 | Regulation S-K, Item 101(c) issues | 764 |
| 59427 | 1 | Regulation S-K, Item 303 issues | 773 |

Displaying records 1 - 10

If you want to check whether two delimited fields behave like parallel
lists, compare their lengths first:

``` sql
SELECT
    comment_response_key,
    cardinality(
        regexp_split_to_array(
            regexp_replace(question_issue_text_list, '(^\|+|\|+$)', '', 'g'),
            '\|+'
        )
    ) AS n_text,
    cardinality(
        regexp_split_to_array(
            regexp_replace(question_issue_key_list, '(^\|+|\|+$)', '', 'g'),
            '\|+'
        )
    ) AS n_key
FROM audit.feed40_comment_letter_threads
WHERE question_issue_text_list IS NOT NULL
   OR question_issue_key_list IS NOT NULL;
```

| comment_response_key | n_text | n_key |
|---------------------:|-------:|------:|
|                53810 |      2 |     2 |
|                53811 |      2 |     2 |
|                53813 |      1 |     1 |
|                53815 |      1 |     1 |
|                53819 |      1 |     1 |
|                53821 |      2 |     2 |
|                53822 |      2 |     2 |
|                53827 |      2 |     2 |
|                53159 |      1 |     1 |
|               214975 |      2 |     2 |

Displaying records 1 - 10

For integer identifiers:

``` sql
SELECT
    cl_con_id,
    array_remove(string_to_array(iss_sec_keys, '|', ''), NULL)::integer[] AS iss_sec_keys_arr
FROM audit.feed25_comment_letters
WHERE iss_sec_keys IS NOT NULL;
```

| cl_con_id | iss_sec_keys_arr |
|----------:|:-----------------|
|     11143 | {2699,2701,2783} |
|     91130 | {3337}           |
|     95802 | {3337}           |
|     72439 | {2525,2526}      |
|    125767 | {2526}           |
|    125767 | {2526}           |
|    187602 | {2526}           |
|    187602 | {2526}           |
|    115753 | {2095,3271}      |
|      2534 | {2526}           |

Displaying records 1 - 10

If you want to ignore empty strings explicitly, use:

``` sql
SELECT
    cl_con_id,
    iss_sec_keys AS iss_sec_keys_raw,
    array_remove(string_to_array(iss_sec_keys, '|', ''), NULL)::integer[] AS iss_sec_keys_arr
FROM audit.feed25_comment_letters
WHERE iss_sec_keys IS NOT NULL;
```

| cl_con_id | iss_sec_keys_raw | iss_sec_keys_arr |
|----------:|:-----------------|:-----------------|
|     11531 | \|2249\|         | {2249}           |
|     11531 | \|2249\|         | {2249}           |
|     82083 | \|2268\|         | {2268}           |
|     82083 | \|2589\|         | {2589}           |
|     82083 | \|2589\|         | {2589}           |
|     82083 | \|2268\|         | {2268}           |
|     32976 | \|2534\|         | {2534}           |
|     32976 | \|2534\|         | {2534}           |
|     60870 | \|2526\|2068\|   | {2526,2068}      |
|     60870 | \|2526\|2068\|   | {2526,2068}      |

Displaying records 1 - 10
