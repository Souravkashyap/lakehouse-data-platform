{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='as_of_month'
) }}
-- Roll-rate matrix: where loans in each DPD bucket at the previous month-end are at this month-end.
-- Roll rate = loans (or POS) moving from a bucket to a worse one in the month / loans in the from-bucket.
-- Needs the month-end rows of fct_lending_loan_daily, which are kept 5 years. Rows are produced only when the
-- run date is a month end; on other days the result is empty and nothing is replaced. Design: section 7.
{% set run_date = require_run_date() %}

with prev as (

    select loan_id, lender_id, product_code, dpd_bucket, pos_paise
    from {{ ref('fct_lending_loan_daily') }}
    where as_of_date = last_day(dateadd(month, -1, '{{ run_date }}'::date))

),

cur as (

    select loan_id, dpd_bucket
    from {{ ref('fct_lending_loan_daily') }}
    where as_of_date = '{{ run_date }}'::date

)

select
    date_trunc(month, '{{ run_date }}'::date) as as_of_month,
    p.lender_id,
    p.product_code,
    p.dpd_bucket                              as from_bucket,
    coalesce(c.dpd_bucket,
             case when lc.written_off_date <= '{{ run_date }}'::date then 'WRITTEN_OFF'
                  when lc.closed_date      <= '{{ run_date }}'::date then 'CLOSED'
                  else 'NOT_IN_SNAPSHOT' end)    as to_bucket,   -- a gap in the daily build shows up, never reads as a closure
    count(*)                                  as loans,
    sum(p.pos_paise)                          as from_pos_paise
from prev as p
left join cur as c on c.loan_id = p.loan_id
left join {{ ref('silver_lending_loans_current') }} as lc on lc.loan_id = p.loan_id
where '{{ run_date }}'::date = last_day('{{ run_date }}'::date)
group by 1, 2, 3, 4, 5
