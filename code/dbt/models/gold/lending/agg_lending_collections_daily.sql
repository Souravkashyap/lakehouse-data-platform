{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='due_date'
) }}
-- Collections by installment due date, lender and product. The last 35 days of due dates are rebuilt every run,
-- because payments keep arriving after the due date. On-time collection efficiency = paid_on_time_paise /
-- emi_due_paise; bounce rate = bounced_installments / installments_due (auto-debit failures are the earliest
-- delinquency signal). Cancelled (cooling-off) loans are excluded. Design: section 7.
{% set run_date = require_run_date() %}

with inst as (

    select
        *,
        convert_timezone('UTC', 'Asia/Kolkata', fully_paid_at)::date as paid_date
    from {{ ref('silver_lending_installments') }}
    where loan_status <> 'CANCELLED'
      and due_date between dateadd(day, -35, '{{ run_date }}'::date) and '{{ run_date }}'::date

)

select
    due_date,
    lender_id,
    product_code,
    count(*)                                                    as installments_due,
    sum(emi_due_paise)                                          as emi_due_paise,
    count_if(paid_date <= due_date)                             as paid_on_time_installments,
    sum(iff(paid_date <= due_date, emi_due_paise, 0))           as paid_on_time_paise,
    count_if(paid_date <= '{{ run_date }}'::date)               as paid_by_measure_installments,
    sum(least(paid_paise, emi_due_paise))                       as collected_paise,
    count_if(bounce_count > 0)                                  as bounced_installments,
    '{{ run_date }}'::date                                      as measured_on
from inst
group by due_date, lender_id, product_code
