{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='as_of_date'
) }}
-- Portfolio by day, lender, product and DPD bucket. Additive measures only (loans, POS, overdue), so any
-- grouping re-aggregates correctly. PAR30 = POS in SMA_1 + SMA_2 + NPA / total POS: computed in the semantic
-- layer, never stored. Builds one date: the run's logical date. Design: section 7.
{% set run_date = require_run_date() %}

select
    as_of_date,
    lender_id,
    product_code,
    dpd_bucket,
    count(*)           as loans,
    sum(pos_paise)     as pos_paise,
    sum(overdue_paise) as overdue_paise
from {{ ref('fct_lending_loan_daily') }}
where as_of_date = '{{ run_date }}'::date
group by as_of_date, lender_id, product_code, dpd_bucket
