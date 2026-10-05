{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='submitted_date'
) }}
-- Application funnel, cohorted by submission date. Applications mature within about 30 days, so the last 30
-- days are rebuilt every run; counts for a recent date grow as applications progress (measured_on says when).
-- Design: section 7.
{% set run_date = require_run_date() %}

select
    submitted_date,
    product_code,
    channel,
    count(*)                                            as applications,
    count_if(approved_amount_paise is not null)         as approved,
    count_if(status = 'REJECTED')                       as rejected,
    count_if(accepted_at is not null)                   as accepted,
    count_if(disbursed_at is not null)                  as disbursed,
    count_if(status in ('CANCELLED', 'EXPIRED'))        as dropped,
    sum(requested_amount_paise)                         as requested_amount_paise,
    sum(approved_amount_paise)                          as approved_amount_paise,
    sum(iff(disbursed_at is not null, datediff(minute, submitted_at, disbursed_at), 0)) as sum_minutes_submit_to_disburse,
    '{{ run_date }}'::date                              as measured_on
from {{ ref('silver_lending_applications_current') }}
where submitted_date between dateadd(day, -30, '{{ run_date }}'::date) and '{{ run_date }}'::date
group by submitted_date, product_code, channel
