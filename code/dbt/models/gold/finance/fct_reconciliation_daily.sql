{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='business_date'
) }}
-- Daily reconciliation status per unit, vendor, day and side, in exact paise. Design: section 9.
-- Rebuilds only the business dates the matching engine re-reconciled in this run (the window plus any
-- partner restatements), never the 5-year history.
{% set run_date = require_run_date() %}
select
    business_unit,
    vendor,
    business_date,
    side,
    count(*)                                                                         as total_items,
    sum(amount_paise)                                                                as total_paise,
    count_if(result in ('MATCHED_EXACT', 'MATCHED_SECONDARY'))                       as matched_items,
    coalesce(sum(case when result in ('MATCHED_EXACT', 'MATCHED_SECONDARY') then amount_paise end), 0) as matched_paise,
    count_if(result not in ('MATCHED_EXACT', 'MATCHED_SECONDARY'))                   as break_items,
    coalesce(sum(case when result not in ('MATCHED_EXACT', 'MATCHED_SECONDARY') then amount_paise end), 0) as break_paise,
    case when count_if(result not in ('MATCHED_EXACT', 'MATCHED_SECONDARY')) = 0
         then 'RECONCILED' else 'BREAKS_OPEN' end                                    as status
from {{ ref('fct_reconciliation_items') }}
where business_date in (
    select distinct business_date from {{ ref('fct_reconciliation_items') }}
    where reconciled_for_run_date = '{{ run_date }}'::date
)
group by business_unit, vendor, business_date, side
