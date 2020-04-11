# Audit Analytics (`audit`)

Audit Analytics contains the severals data sets.
Unfortunately, the tables provided by WRDS are a complete mess and the data are often not well formatted.
One particular issue is that the tables are very much *not* normalized.
So there is a lot of duplicated data and tables are loaded with extraneous information.


## Director and officer changes

Associated tables and changes from WRDS versions are:

- `diroffichange`: 
    - Removed `prior: match: closest:` fields 
    - Removed `do_change_text:` fields. My assumption is that the `do_change_text:` fields are derived from the `director_officer_change_text` field on `feed17change`.
    - In addition to the `is_` fields, `interim`, `do_off_remains`, `retain_other_pos`, `eff_date_unspec`, and `eff_date_next_meet` are all made explicitly boolean.
- `feed17change`: The field `director_officer_change_key` is renamed to `do_off_pers_key` to align with the field on `diroffichange`.
- `feed17del`: I *guess* this table identifies observations found in `feed17change`, but not  `diroffichange`. If so, I'm not sure why these are deleted.

