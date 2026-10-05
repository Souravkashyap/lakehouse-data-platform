{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='disbursal_date'
) }}
-- Disbursals by IST day, lender, product and channel. The last 8 days are rebuilt every run because late events
-- and cooling-off cancellations change old days. Additive measures only: average ticket = principal_paise /
-- loans_disbursed; weighted rate = sum_rate_bps_x_principal_paise / principal_paise. Both re-aggregate
-- correctly across any grouping. Design: section 7.
{% set run_date = require_run_date() %}

select
    l.disbursal_date,
    l.lender_id,
    l.product_code,
    coalesce(a.channel, 'UNKNOWN')                     as channel,
    count(*)                                           as loans_disbursed,
    sum(l.principal_paise)                             as principal_paise,
    sum(l.processing_fee_paise)                        as processing_fee_paise,
    sum(l.net_disbursed_paise)                         as net_disbursed_paise,
    sum(l.tenure_months)                               as sum_tenure_months,
    sum(l.interest_rate_bps * l.principal_paise)       as sum_rate_bps_x_principal_paise,
    count_if(l.status = 'CANCELLED')                   as loans_cancelled_cooling_off,
    sum(iff(l.status = 'CANCELLED', l.principal_paise, 0)) as principal_cancelled_paise
from {{ ref('silver_lending_loans_current') }} as l
left join {{ ref('silver_lending_applications_current') }} as a on a.application_id = l.application_id
where l.disbursal_date between dateadd(day, -7, '{{ run_date }}'::date) and '{{ run_date }}'::date
group by l.disbursal_date, l.lender_id, l.product_code, coalesce(a.channel, 'UNKNOWN')
