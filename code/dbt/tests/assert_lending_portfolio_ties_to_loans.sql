-- docs/test-plan.md test 18: portfolio totals tie to the loan-level snapshot (loans, POS, overdue).
-- Returns rows only when the aggregate dropped or double-counted loans on any of the last 8 days.
-- Design: section 7.
{% set run_date = require_run_date() %}

with loan_level as (

    select as_of_date, lender_id, product_code, dpd_bucket,
           count(*) as loans, sum(pos_paise) as pos_paise, sum(overdue_paise) as overdue_paise
    from {{ ref('fct_lending_loan_daily') }}
    where as_of_date between dateadd(day, -7, '{{ run_date }}'::date) and '{{ run_date }}'::date
    group by all

),

portfolio as (

    select as_of_date, lender_id, product_code, dpd_bucket, loans, pos_paise, overdue_paise
    from {{ ref('agg_lending_portfolio_daily') }}
    where as_of_date between dateadd(day, -7, '{{ run_date }}'::date) and '{{ run_date }}'::date

)

select
    coalesce(l.as_of_date, p.as_of_date)     as as_of_date,
    coalesce(l.lender_id, p.lender_id)       as lender_id,
    coalesce(l.product_code, p.product_code) as product_code,
    coalesce(l.dpd_bucket, p.dpd_bucket)     as dpd_bucket,
    l.loans as loan_level_loans, p.loans as portfolio_loans,
    l.pos_paise as loan_level_pos_paise, p.pos_paise as portfolio_pos_paise,
    l.overdue_paise as loan_level_overdue_paise, p.overdue_paise as portfolio_overdue_paise
from loan_level as l
full outer join portfolio as p
    on  l.as_of_date = p.as_of_date
    and l.lender_id = p.lender_id
    and l.product_code = p.product_code
    and l.dpd_bucket = p.dpd_bucket
where coalesce(l.loans, 0) <> coalesce(p.loans, 0)
   or coalesce(l.pos_paise, 0) <> coalesce(p.pos_paise, 0)
   or coalesce(l.overdue_paise, 0) <> coalesce(p.overdue_paise, 0)
