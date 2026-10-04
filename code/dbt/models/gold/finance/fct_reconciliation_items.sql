{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='business_date'
) }}
-- Matching engine. Guarantee: every internal money movement and every partner TRANSACTION line appears
-- exactly once, with one result. Zero amount tolerance: tolerance applies to time, never to paise. Design: section 9.
{% set run_date = require_run_date() %}
-- The window is keyed to the RUN's logical date. Using current_date here would make re-runs and backfills
-- compute the wrong window.

with restated as (

    -- business dates the partner restated in snapshots inside the window: re-reconcile them too
    select business_date as restated_date from {{ ref('silver_partner_record_changes') }}
    where change_date between dateadd(day, -7, '{{ run_date }}'::date) and '{{ run_date }}'::date
    union
    select prior_business_date from {{ ref('silver_partner_record_changes') }}
    where prior_business_date is not null
      and change_date between dateadd(day, -7, '{{ run_date }}'::date) and '{{ run_date }}'::date

),

internal as (

    select
        'INTERNAL' as side, item_id, business_unit, vendor, business_date,
        payment_ref, customer_id, amount_paise, null as vendor_status,
        row_number() over (
            partition by vendor, coalesce(payment_ref, 'none:' || item_id)
            order by event_time_utc, item_id
        ) as dup_rank
    from {{ ref('silver_money_movements') }}
    where vendor is not null
      and (business_date between dateadd(day, -9, '{{ run_date }}'::date) and dateadd(day, 2, '{{ run_date }}'::date)
           or exists (select 1 from restated as r where abs(datediff(day, r.restated_date, business_date)) <= 2))

),

partner as (

    select
        'PARTNER' as side, partner_line_id as item_id, business_unit, vendor, business_date,
        payment_ref, customer_id, amount_paise, vendor_status,
        row_number() over (
            partition by vendor, coalesce(payment_ref, 'none:' || partner_line_id)
            order by business_date, partner_line_id
        ) as dup_rank
    from {{ ref('silver_partner_records') }}
    where line_type = 'TRANSACTION'
      and (business_date between dateadd(day, -9, '{{ run_date }}'::date) and dateadd(day, 2, '{{ run_date }}'::date)
           or exists (select 1 from restated as r where abs(datediff(day, r.restated_date, business_date)) <= 2))

),

-- one-to-one pairing on (vendor, payment_ref, dup_rank): no join fan-out
exact_pairs as (

    select
        i.item_id as i_item_id,
        p.item_id as p_item_id,
        case
            when i.amount_paise <> p.amount_paise then 'AMOUNT_DIFFERS'
            when p.vendor_status in ('FAILED', 'REVERSED') then 'STATUS_DISAGREES'
            else 'MATCHED_EXACT'
        end as result
    from internal as i
    inner join partner as p
        on  i.vendor = p.vendor
        and i.payment_ref = p.payment_ref
        and i.dup_rank = p.dup_rank

),

-- a repeat of a (vendor, payment_ref) already present at rank 1 on its own side is a DUPLICATE
internal_left as (
    select i.* from internal as i
    where i.dup_rank = 1
      and not exists (select 1 from exact_pairs as e where e.i_item_id = i.item_id)
),

partner_left as (
    select p.* from partner as p
    where p.dup_rank = 1
      and not exists (select 1 from exact_pairs as e where e.p_item_id = p.item_id)
),

secondary_candidates as (

    select
        i.item_id as i_item_id,
        p.item_id as p_item_id,
        abs(datediff(day, i.business_date, p.business_date)) as day_gap
    from internal_left as i
    inner join partner_left as p
        on  i.vendor = p.vendor
        and i.customer_id = p.customer_id
        and i.amount_paise = p.amount_paise           -- exact paise, always
        and abs(datediff(day, i.business_date, p.business_date)) <= 2

),

-- keep only mutual-best pairs so one item can never pair twice
secondary_pairs as (

    select i_item_id, p_item_id, 'MATCHED_SECONDARY' as result
    from (
        select
            *,
            row_number() over (partition by i_item_id order by day_gap, p_item_id) as rn_internal,
            row_number() over (partition by p_item_id order by day_gap, i_item_id) as rn_partner
        from secondary_candidates
    )
    where rn_internal = 1 and rn_partner = 1

),

pairs as (
    select * from exact_pairs
    union all
    select * from secondary_pairs
),

paired_internal as (
    select i.side, i.item_id, md5(pr.i_item_id || '|' || pr.p_item_id) as match_pair_id,
           i.business_unit, i.vendor, i.business_date, i.payment_ref, i.amount_paise, pr.result
    from pairs as pr inner join internal as i on i.item_id = pr.i_item_id
),

paired_partner as (
    select p.side, p.item_id, md5(pr.i_item_id || '|' || pr.p_item_id) as match_pair_id,
           p.business_unit, p.vendor, p.business_date, p.payment_ref, p.amount_paise, pr.result
    from pairs as pr inner join partner as p on p.item_id = pr.p_item_id
),

unpaired_internal as (
    select side, item_id, null as match_pair_id, business_unit, vendor, business_date, payment_ref, amount_paise,
        case
            when dup_rank > 1 then 'DUPLICATE'
            when business_date >= dateadd(day, -1, '{{ run_date }}'::date) then 'TIMING'  -- partner may report T+1
            else 'MISSING_AT_PARTNER'
        end as result
    from internal as i
    where not exists (select 1 from pairs as pr where pr.i_item_id = i.item_id)
),

unpaired_partner as (
    select side, item_id, null as match_pair_id, business_unit, vendor, business_date, payment_ref, amount_paise,
        case when dup_rank > 1 then 'DUPLICATE' else 'MISSING_INTERNALLY' end as result
    from partner as p
    where not exists (select 1 from pairs as pr where pr.p_item_id = p.item_id)
),

all_items as (
    select * from paired_internal
    union all select * from paired_partner
    union all select * from unpaired_internal
    union all select * from unpaired_partner
)

select
    side || ':' || item_id as item_key,
    side,
    item_id,
    match_pair_id,
    business_unit,
    vendor,
    business_date,
    payment_ref,
    amount_paise,
    result,
    '{{ run_date }}'::date as reconciled_for_run_date
from all_items
-- only window dates and restated dates are output, so delete+insert replaces exactly those dates
where business_date between dateadd(day, -7, '{{ run_date }}'::date) and '{{ run_date }}'::date
   or business_date in (select restated_date from restated)
