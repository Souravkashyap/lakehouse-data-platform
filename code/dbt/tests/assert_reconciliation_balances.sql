-- docs/test-plan.md test 5: matched + breaks = total. Returns rows only when reconciliation lost or double-counted money.
-- Source totals are recomputed independently of the matching engine. Design: section 9.
with covered as (

    select min(business_date) as min_date, max(business_date) as max_date
    from {{ ref('fct_reconciliation_items') }}

),

source_totals as (

    select business_unit, vendor, business_date, 'INTERNAL' as side,
           count(*) as src_items, sum(amount_paise) as src_paise
    from {{ ref('silver_money_movements') }}
    where vendor is not null
      and business_date between (select min_date from covered) and (select max_date from covered)
    group by all

    union all

    select business_unit, vendor, business_date, 'PARTNER' as side,
           count(*), sum(amount_paise)
    from {{ ref('silver_partner_records') }}
    where line_type = 'TRANSACTION'
      and business_date between (select min_date from covered) and (select max_date from covered)
    group by all

),

item_totals as (

    select business_unit, vendor, business_date, side,
           count(*) as item_count,
           sum(amount_paise) as item_paise,
           coalesce(sum(case when result in ('MATCHED_EXACT', 'MATCHED_SECONDARY') then amount_paise end), 0) as matched_paise,
           coalesce(sum(case when result not in ('MATCHED_EXACT', 'MATCHED_SECONDARY') then amount_paise end), 0) as break_paise
    from {{ ref('fct_reconciliation_items') }}
    group by all

)

select
    coalesce(s.business_unit, i.business_unit) as business_unit,
    coalesce(s.vendor, i.vendor)               as vendor,
    coalesce(s.business_date, i.business_date) as business_date,
    coalesce(s.side, i.side)                   as side,
    s.src_items, i.item_count,
    s.src_paise, i.item_paise, i.matched_paise, i.break_paise
from source_totals as s
full outer join item_totals as i
    on  s.business_unit = i.business_unit
    and s.vendor = i.vendor
    and s.business_date = i.business_date
    and s.side = i.side
where coalesce(s.src_items, 0) <> coalesce(i.item_count, 0)
   or coalesce(s.src_paise, 0) <> coalesce(i.item_paise, 0)
   or coalesce(i.matched_paise, 0) + coalesce(i.break_paise, 0) <> coalesce(i.item_paise, 0)
