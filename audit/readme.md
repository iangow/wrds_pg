# Audit Analytics (`audit`)

Audit Analytics contains the severals data sets.
Unfortunately, the tables provided by WRDS are a complete mess and the data are often not well formatted.
One particular issue is that the tables are very much *not* normalized.
So there is a lot of duplicated data and tables are loaded with extraneous information.

Audit Analytics comprises five sets of data:

1. Audit and Compliance
2. Corporate and Legal
3. Accounting and Oversight
4. Other Independent Audit
5. Canada (SEDAR)

The University of Melbourne only has subscriptions to the first two sets of data

## Audit and Compliance

The 14 tables provided by WRDS in this data set are listed below (with the WRDS table names listed in parentheses).
In each case, the MCCGR database includes a table with the same name, but in general we do not include *all* variables that provided in the WRDS version of the table.

### Omission of financial variables

Specifically, the WRDS tables often include variables from the "company financial block", which are financial statement variables such as net income or total assets for the "`closest`", "`match`", "`hiwater`", or "`prior`" periods (either quarters [`qu`] or years [`yr`]).
These variables expand the size of the tables dramatically and their provenance is unclear (likely scraped from SEC filings, which the main source for Audit Analytics data).
Also, the meaning of the terms "`closest`", "`match`", "`hiwater`", and "`prior`" is unclear and the data are very poorly documented on WRDS.
So, we simply omit these variables when importing the data.

### Special handling of textual variables

Several tables include fields with the text (or portions of the text) of the underlying SEC filings.
Presumably due to constraints on field size in SAS, these text variables are split into a number of columns.
For example, the text in filings related to rows in `diroffichange` is found in `do_change_text1`, `do_change_text2`, and `do_change_text3`. It is likely that pasting `do_change_text1`, `do_change_text2`, and `do_change_text3` back together, one gets the original text that was fed to WRDS by Audit Analytics.

In addition to the data being split across a number of columns, the textual data often causes problems in importing tables.
For this reason, often the textual data is placed in a separate table from the other data (and in some cases, a number of separate tables).
For example, the variables `do_change_text1`, `do_change_text2`, and `do_change_text3` are found in tables `diroffichange_text1`, `diroffichange_text2`, and `diroffichange_text3` respectively along with the primary key variable for the original table (`do_off_pers_key` in the case of `diroffichange`).

Separate text tables are provided for the following tables:

- `auditsox302`: `auditsox302_text`
- `auditsox404`: `auditsox404_text1`, `auditsox404_text2`
- `diroffichange`: `diroffichange_text1`, `diroffichange_text2`, `diroffichange_text3`  
- `nt`: `nt_text1`, `nt_text2`, `nt_text3` 

Here is some code illustrating how to merge the tables and combine the textual variables (in this case, for `auditsox404` data).

``` r
library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres(), bigint="integer")
rs <- dbExecute(pg, "SET work_mem TO '8GB'")
rs <- dbExecute(pg, "SET search_path TO audit")

auditsox404_text1 <- tbl(pg, "auditsox404_text1")
auditsox404_text2 <- tbl(pg, "auditsox404_text2")

auditsox404_text <-
    auditsox404_text1 %>%
    inner_join(auditsox404_text2) %>%
    mutate(ic_op_text = trimws(paste(coalesce(ic_text1, ""),
                                     coalesce(ic_text2, "")))) %>%
    select(ic_op_fkey, ic_op_text) %>%
    compute()
#> Joining, by = "ic_op_fkey"
auditsox404_text
#> # Source:   table<dbplyr_002> [?? x 2]
#> # Database: postgres [igow@/var/run/postgresql:5434/crsp]
#>    ic_op_fkey ic_op_text                                                        
#>         <int> <chr>                                                             
#>  1      19322 "Report of Independent Registered Public Accounting Firm   The Bo…
#>  2      19323 "Management\u0092s Report on Internal Control Over Financial Repo…
#>  3      29580 "Report of Independent Registered Public Accounting Firm      The…
#>  4      29581 "Management\u0092s Report on Internal Control Over Financial Repo…
#>  5      45178 "Report of Independent Registered Public Accounting Firm <p>     …
#>  6      45179 "Management\u0092s Report on Internal Control Over Financial Repo…
#>  7      60585 "Management\u0092s Report on Internal Control Over Financial Repo…
#>  8      26515 "REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM <p>To th…
#>  9      99563 "Report of Independent Registered Public Accounting Firm<p>To the…
#> 10      60584 "Report of Independent Registered Public Accounting Firm  The Boa…
#> # … with more rows
```

<sup>Created on 2020-06-12 by the [reprex package](https://reprex.tidyverse.org) (v0.3.0)</sup>

And here is similar code for `diroffichange` text variables.
Note that there is a lot of duplication in the underlying table, as one filing may relate to multiple
directors and officers.

``` r
library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres(), bigint="integer")
rs <- dbExecute(pg, "SET work_mem TO '8GB'")
rs <- dbExecute(pg, "SET search_path TO audit")

diroffichange_text1 <- tbl(pg, "diroffichange_text1")
diroffichange_text2 <- tbl(pg, "diroffichange_text2")
diroffichange_text3 <- tbl(pg, "diroffichange_text3")

dotext_1 <-
    diroffichange_text1 %>%
    distinct()

diroffichange_text <-
    diroffichange_text1 %>% distinct() %>%
    inner_join(diroffichange_text2 %>% distinct()) %>%
    inner_join(diroffichange_text3 %>% distinct()) %>%
    mutate(do_change_text = trimws(paste(coalesce(do_change_text1, ""),
                                         coalesce(do_change_text2, ""),
                                         coalesce(do_change_text3, "")))) %>%
    select(ftp_file_fkey, do_change_text) %>%
    compute()
#> Joining, by = "ftp_file_fkey"
#> Joining, by = "ftp_file_fkey"
diroffichange_text
#> # Source:   table<dbplyr_002> [?? x 2]
#> # Database: postgres [igow@/var/run/postgresql:5434/crsp]
#>    ftp_file_fkey                 do_change_text                                 
#>    <chr>                         <chr>                                          
#>  1 edgar/data/1000015/000110465… "Item 5.02.   Departure of Directors or Princi…
#>  2 edgar/data/1000045/000100004… "Item 6.  Other Events<p>   On May 20, 2002 Ni…
#>  3 edgar/data/1000045/000119312… "Item 5.02 Departure of Directors or Certain O…
#>  4 edgar/data/1000045/000119312… "Item 5.02 Departure of Directors or Certain O…
#>  5 edgar/data/1000045/000119312… "Item 5.02 Departure of Directors or Certain O…
#>  6 edgar/data/1000045/000119312… "Item 5.02 Departure of Directors or Certain O…
#>  7 edgar/data/1000045/000119312… "Item 5.02. Departure of Directors or Certain …
#>  8 edgar/data/1000045/000119312… "Item 5.02. Departure of Directors or Certain …
#>  9 edgar/data/1000045/000119312… "Item 5.02. Departure of Directors or Certain …
#> 10 edgar/data/1000045/000119312… "Item 5.02. Departure of Directors or Certain …
#> # … with more rows
```

<sup>Created on 2020-06-12 by the [reprex package](https://reprex.tidyverse.org) (v0.3.0)</sup>

Finally, the text associated with `nt` is a bit different, as most rows have no data (so a `LEFT JOIN` might be needed here):

``` r
library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres(), bigint="integer")
rs <- dbExecute(pg, "SET work_mem TO '8GB'")
rs <- dbExecute(pg, "SET search_path TO audit")

nt_text1 <- tbl(pg, "nt_text1")
nt_text2 <- tbl(pg, "nt_text2")
nt_text3 <- tbl(pg, "nt_text3")

nt_text <-
    nt_text1 %>%
    filter(!is.na(part4_3_text1)) %>%
    inner_join(nt_text2) %>%
    inner_join(nt_text3) %>%
    mutate(part4_3_text = trimws(paste(coalesce(part4_3_text1, ""),
                                         coalesce(part4_3_text2, ""),
                                         coalesce(part4_3_text3, "")))) %>%
    select(nt_notify_key, part4_3_text) %>%
    compute()
#> Joining, by = "nt_notify_key"
#> Joining, by = "nt_notify_key"
nt_text
#> # Source:   table<dbplyr_002> [?? x 2]
#> # Database: postgres [igow@/var/run/postgresql:5434/crsp]
#>    nt_notify_key part4_3_text                                                   
#>            <int> <chr>                                                          
#>  1         18350 "no"                                                           
#>  2         60786 "As stated in the Company\u0092s Annual Report on Form 10-K fo…
#>  3         46351 "In the second quarter of 2000 revenues were $333,741 compared…
#>  4         39670 "It is anticipated that the Company's Quarterly Report for the…
#>  5         77827 "Revenues for the quarter decreased by $38 to $0 and the net l…
#>  6        101513 "The Company does not expect significant changes in its revenu…
#>  7         45567 "Yes but no narrative."                                        
#>  8        105854 "For the fiscal year ended June 30, 2017, the Registrant recor…
#>  9        106610 "See Exhibit A attached hereto, which Exhibit is incorporated …
#> 10         15217 "Certain matters discussed in this Notification of Late Filing…
#> # … with more rows
nt_text1 %>% count()
#> Warning: The `add` argument of `group_by()` is deprecated as of dplyr 1.0.0.
#> Please use the `.add` argument instead.
#> This warning is displayed once every 8 hours.
#> Call `lifecycle::last_warnings()` to see where this warning was generated.
#> # Source:   lazy query [?? x 1]
#> # Database: postgres [igow@/var/run/postgresql:5434/crsp]
#>        n
#>    <int>
#> 1 109924
nt_text %>% count()
#> # Source:   lazy query [?? x 1]
#> # Database: postgres [igow@/var/run/postgresql:5434/crsp]
#>       n
#>   <int>
#> 1 26128
```

<sup>Created on 2020-06-12 by the [reprex package](https://reprex.tidyverse.org) (v0.3.0)</sup>

### Omission of auditor names from some tables

The WRDS tables include auditor names as well as an auditor key (i.e., `auditor_key` as found on `auditorsinfo`).
Because the `auditor_name` appears on `auditors`, it is redundant to include the same information on each table in which `auditor_key` appears. 
So we have deleted it from many tables.
Simply join the table with `auditors` using `auditor_key` to recover this variable.

- Auditors (`auditorsinfo`)
- Auditor Changes (`auditchange`)
- Audit Fees (`auditfees`)
- Audit Fees with Restatements (`auditfeesr`)
- Audit Opinions (`auditopin`)
- Revised Audit Opinions (`revauditopin`)
- Benefit Plan Opinions (`benefit`)
- Current Auditors (`auditors`)
- Non-Reliance Restatements (`auditnonreli`)
- SOX 302 Disclosure Controls (`auditsox302`)
- SOX 404 Internal Controls (`auditsox402`)
- Accelerated Filer (`accfiler`)
- Director and Officer Changes (`diroffichange`)
- Non-timely Filer Information And Analysis (`nt`)
