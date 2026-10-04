{{ config(materialized='view', table_format='default') }}
-- One row per unmatched or disagreeing item, with an owner and age. Design: section 9.
select
    i.*,
    'vendor-ops:' || i.vendor as owner,
    datediff(day, i.business_date, '{{ require_run_date() }}'::date) as age_days
from {{ ref('fct_reconciliation_items') }} as i
where i.result not in ('MATCHED_EXACT', 'MATCHED_SECONDARY')
