{{ config(materialized='table') }}
-- Vintage curves: for each disbursal-month cohort, lender and product, how many loans (and how much principal)
-- were EVER 30+ and 90+ days past due by month-on-book n, plus first-payment-default at 30 days (fpd30).
-- "Ever" (not "currently") because it never decreases and compares cohorts fairly. Rates are computed in the
-- semantic layer: principal_ever_dpd31_paise / cohort_principal_paise. Full rebuild each day from the
-- milestones table: one pass, small output. The current month's last point is partial (is_partial_month).
-- Cancelled (cooling-off) loans are excluded. Design: section 7.
{% set run_date = require_run_date() %}

with loans as (

    select *
    from {{ ref('fct_lending_loan_milestones') }}
    where not is_cancelled_cooling_off
      and cohort_month <= date_trunc(month, '{{ run_date }}'::date)

),

hits as (

    -- a milestone counts only once its date has passed (projections are not facts)
    select
        cohort_month,
        lender_id,
        product_code,
        principal_paise,
        iff(first_dpd31_date <= '{{ run_date }}'::date, datediff(month, cohort_month, first_dpd31_date), null) as mob_dpd31,
        iff(first_dpd91_date <= '{{ run_date }}'::date, datediff(month, cohort_month, first_dpd91_date), null) as mob_dpd91,
        iff(fpd30_date <= '{{ run_date }}'::date, datediff(month, cohort_month, fpd30_date), null)             as mob_fpd30
    from loans

),

cohorts as (

    select
        cohort_month,
        lender_id,
        product_code,
        count(*)             as cohort_loans,
        sum(principal_paise) as cohort_principal_paise
    from loans
    group by cohort_month, lender_id, product_code

),

mobs as (

    select row_number() over (order by seq4()) - 1 as mob
    from table(generator(rowcount => 61))

),

grid as (

    select c.*, m.mob
    from cohorts as c
    cross join mobs as m
    where dateadd(month, m.mob, c.cohort_month) <= date_trunc(month, '{{ run_date }}'::date)

),

new_dpd31 as (

    select cohort_month, lender_id, product_code, mob_dpd31 as mob,
           count(*) as loans_new, sum(principal_paise) as principal_new
    from hits
    where mob_dpd31 is not null
    group by cohort_month, lender_id, product_code, mob_dpd31

),

new_dpd91 as (

    select cohort_month, lender_id, product_code, mob_dpd91 as mob,
           count(*) as loans_new, sum(principal_paise) as principal_new
    from hits
    where mob_dpd91 is not null
    group by cohort_month, lender_id, product_code, mob_dpd91

),

new_fpd30 as (

    select cohort_month, lender_id, product_code, mob_fpd30 as mob,
           count(*) as loans_new
    from hits
    where mob_fpd30 is not null
    group by cohort_month, lender_id, product_code, mob_fpd30

),

joined as (

    select
        g.*,
        coalesce(a.loans_new, 0)     as new_loans_dpd31,
        coalesce(a.principal_new, 0) as new_principal_dpd31,
        coalesce(b.loans_new, 0)     as new_loans_dpd91,
        coalesce(b.principal_new, 0) as new_principal_dpd91,
        coalesce(f.loans_new, 0)     as new_loans_fpd30
    from grid as g
    left join new_dpd31 as a
        on  a.cohort_month = g.cohort_month and a.lender_id = g.lender_id
        and a.product_code = g.product_code and a.mob = g.mob
    left join new_dpd91 as b
        on  b.cohort_month = g.cohort_month and b.lender_id = g.lender_id
        and b.product_code = g.product_code and b.mob = g.mob
    left join new_fpd30 as f
        on  f.cohort_month = g.cohort_month and f.lender_id = g.lender_id
        and f.product_code = g.product_code and f.mob = g.mob

),

{% set to_date_by_mob = "over (partition by cohort_month, lender_id, product_code order by mob rows between unbounded preceding and current row)" %}
curves as (

    -- "ever by MOB n" = running total of first-time hits up to month n
    select
        cohort_month,
        lender_id,
        product_code,
        mob,
        cohort_loans,
        cohort_principal_paise,
        sum(new_loans_dpd31)     {{ to_date_by_mob }} as loans_ever_dpd31,
        sum(new_principal_dpd31) {{ to_date_by_mob }} as principal_ever_dpd31_paise,
        sum(new_loans_dpd91)     {{ to_date_by_mob }} as loans_ever_dpd91,
        sum(new_principal_dpd91) {{ to_date_by_mob }} as principal_ever_dpd91_paise,
        sum(new_loans_fpd30)     {{ to_date_by_mob }} as loans_fpd30
    from joined

)

select
    cohort_month,
    lender_id,
    product_code,
    mob,
    cohort_loans,
    cohort_principal_paise,
    loans_ever_dpd31,
    principal_ever_dpd31_paise,
    loans_ever_dpd91,
    principal_ever_dpd91_paise,
    loans_fpd30,
    (dateadd(month, mob, cohort_month) = date_trunc(month, '{{ run_date }}'::date)) as is_partial_month,
    '{{ run_date }}'::date as as_of_date
from curves
