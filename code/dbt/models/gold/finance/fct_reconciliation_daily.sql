{{ config(materialized='table') }}
-- Daily reconciliation status per unit, vendor, day and side, in exact paise. Design: section 9.
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
group by business_unit, vendor, business_date, side
