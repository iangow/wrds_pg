# CRSP

Data found in the `crsp` data library on WRDS (`/wrds/crsp/sasdata/` on SAS server)

## Introduction

As discussed [here](http://www.crsp.org/about-crsp), "since 1960, CRSP has provided research-quality data to scholarly researchers and advanced the body of knowledge in finance, economics and related disciplines. Today, nearly 500 leading academic institutions in 35 countries rely on CRSP data for academic research and to support classroom instructions."

## Additional tables

Most of the tables maintained in the `crsp` schema are imported from WRDS using fairly straightforward application of the `wrds_udpate` function from the `wrds2pg` package.
However, there are some additional table that the code in this directory produces:

- `crsp.rets`: Daily stock returns compounded with delisting returns. 
This purpose of this table is to dramatically speed up calculation of event returns, but creation of this table take some time.
This table is used by functions such as `get_event_cum_rets.R` in the `farr` package available [here](https://github.com/iangow/farr/).
- `crsp.mrets`: Monthly stock returns compounded with delisting returns. 
This table is used by functions such as `get_event_cum_rets_mth.R` in the `farr` package available [here](https://github.com/iangow/farr/).
- `tfz_ft`: The table `crsp.tfz_ft` is available through the WRDS web interface, 
but does not exist as a separate SAS file or PostgreSQL table. 
According to WRDS, "behind the scenes the web query form is joining two tables on the fly. 
The tables this query is joining are `crsp.tfz_idx` and either `crsp.tfz_dly_ft` or `crsp.tfz_mth_ft` (depending on whether you want daily or monthly data) by the variable `kytreasnox`."
Here are some links to the information about these tables:
 [`crsp.tfz_idx`](https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77140), 
[`crsp.tfz_dly_ft`](https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77137),
[`crsp.tfz_mth_ft`](https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=137&file_id=77147).

- `trading_dates`: A simple table comprising two fields: `date` and `td`. 
Here, `date` is a trading date available on `crsp.dsi` (or, equivalently, `crsp.dsf`) and `td` is a simple sequence of integers starting at `1` for the first trading date and increasing by one for each subsequent date.
- `anncdates`: A simple table comprising three fields: `anncdate`, `date` and `td`. 
Here, `anncdate` is *any* date on or after `1925-12-31`, whether or not a trading date, and `date` is the next available trading date.
This table allows any date to be mapped to the first date on which trading occurs on or after that date.
See [here](http://iangow.me/far_2021/event-studies.html) for more on the problem that `trading_dates` and `anncdates` are intended to solve and illusrations of their use.
