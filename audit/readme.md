# Audit Analytics

Data found in `audit` data library on WRDS (`/wrds/audit/sasdata/` on
SAS server)

## Introduction

[Audit Analytics](https://www.auditanalytics.com/) “is an independent
research provider of audit, regulatory and disclosure intelligence.
Audit Analytics provides detailed data on over 150,000 active audits and
more than 10,000 accounting firms.” Please refer to the [Manuals and
Overviews](https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/audit-analytics/)
or download a [zip
file](https://github.com/mccgr/wrds_pg/blob/master/audit/AuditAnalyticsManuals.zip?raw=true)
to view the data structures and variable definitions of Audit Analytics
via WRDS.

## List of datasets imported

Audit Analytics comprises five sets of data:

1.  Audit and Compliance
2.  Corporate and Legal
3.  Accounting and Oversight
4.  Other Independent Audit
5.  Canada (SEDAR)

The University of Melbourne currently has subscriptions to the first two
sets of data

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
- SOX 302 Disclosure Controls (`auditsox302`): To be renamed
  `feed10_sox_302_disclosure_contro`?
- SOX 404 Internal Controls (`auditsox302`): To be renamed
  `feed11_sox_404_internal_controls`?
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

In each case, the MCCGR database includes a table with the same name,
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

### Special handling of textual variables

Several tables include fields with the text (or portions of the text) of
the underlying SEC filings. Presumably due to constraints on field size
in SAS, these text variables are split into a number of columns. For
example, the text in filings related to rows in `diroffichange` is found
in `do_change_text1`, `do_change_text2`, and `do_change_text3`. It is
likely that by pasting `do_change_text1`, `do_change_text2`, and
`do_change_text3` back together, one gets the original text that was fed
to WRDS by Audit Analytics.

In addition to the data being split across a number of columns, the
textual data often causes problems in importing tables. For this reason,
often the textual data is placed in a separate table from the other data
(and in some cases, a number of separate tables). For example, the
variables `do_change_text1`, `do_change_text2`, and `do_change_text3`
are found in tables `diroffichange_text1`, `diroffichange_text2`, and
`diroffichange_text3` respectively along with the primary key variable
for the original table (`do_off_pers_key` in the case of
`diroffichange`).

Separate text tables are provided for the following tables:

- `auditsox302`: `auditsox302_text`
- `auditsox404`: `auditsox404_text1`, `auditsox404_text2`
- `feed17_director_and_officer_chan`: Text field is currently omitted
  (see below).

### Omission of auditor names from some tables

The WRDS tables include auditor names as well as an auditor key (i.e.,
`auditor_key` as found on `auditorsinfo`). Because the `auditor_name`
appears on `auditors`, it is redundant to include the same information
on each table in which `auditor_key` appears. So we have deleted it from
many tables. Simply join the table with `auditors` using `auditor_key`
to recover this variable.

## Sample code

### Merge tables and combine textual variables for `feed11_sox_404_internal_controls`

Here is some code illustrating how to merge the tables and combine the
textual variables (in this case, for `auditsox404` data).

``` r
library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres(), bigint="integer")
rs <- dbExecute(pg, "SET search_path TO audit")

auditsox404 <- tbl(pg, "auditsox404")
auditsox404_text1 <- tbl(pg, "auditsox404_text1")
auditsox404_text2 <- tbl(pg, "auditsox404_text2")

auditsox404_text <-
    auditsox404_text1 |>
    inner_join(auditsox404_text2,by = "ic_op_fkey") |>
    mutate(ic_op_text = trimws(paste(coalesce(ic_text1, ""),
                                     coalesce(ic_text2, "")))) |>
    select(ic_op_fkey, ic_op_text)

auditsox404_text
```

    # Source:   SQL [?? x 2]
    # Database: postgres  [igow@/tmp:5432/igow]
       ic_op_fkey ic_op_text                                                        
            <int> <chr>                                                             
     1     132433 "REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM  The Boa…
     2     144022 "The Board of Directors and Shareholders  Arrow Electronics, Inc.…
     3     119508 "MANAGEMENTâ€™S REPORT ON INTERNAL CONTROL OVER FINANCIAL REPORTI…
     4     166216 "Report of Independent Registered Public Accounting Firm  To the …
     5     177185 "Report of Independent Registered Public Accounting Firm  To the …
     6     119507 "REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM  To the …
     7     197806 "Management's Report on Internal Control Over Financial Reporting…
     8     197805 "Report of Independent Registered Public Accounting Firm  To the …
     9     208786 "Report of Independent Registered Public Accounting Firm  To the …
    10      56033 "Managementâ€™s Report on Internal Control over Financial Reporti…
    # ℹ more rows

### Combine textual variables for `feed17_director_and_officer_chan`

The `feed17_director_and_officer_chan` text variables currently do not
work due to the text column being too wide.
