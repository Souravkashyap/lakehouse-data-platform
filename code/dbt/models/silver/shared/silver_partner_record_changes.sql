{{ config(materialized='view', table_format='default') }}
-- Every partner change per snapshot day, classified NEW / CHANGED / REMOVED. Downstream re-reconciles
-- the business dates the partner restated. Design: section 9.
with versions as (

    select
        *,
        lag(dbt_valid_from) over (partition by partner_line_id order by dbt_valid_from) as prev_valid_from,
        lag(business_date)  over (partition by partner_line_id order by dbt_valid_from) as prior_business_date
    from {{ ref('snap_partner_records') }}

),

opened as (

    select
        partner_line_id, vendor, business_date, prior_business_date, amount_paise, vendor_status, row_hash,
        snapshot_date as change_date,
        case when prev_valid_from is null then 'NEW' else 'CHANGED' end as change_type
    from versions

),

removed as (

    -- a closed version with no successor was invalidated: the partner dropped the record
    select
        v.partner_line_id, v.vendor, v.business_date, null as prior_business_date, v.amount_paise, v.vendor_status, v.row_hash,
        to_date(v.dbt_valid_to) as change_date,
        'REMOVED' as change_type
    from versions as v
    where v.dbt_valid_to is not null
      and not exists (
          select 1 from versions as n
          where n.partner_line_id = v.partner_line_id and n.dbt_valid_from = v.dbt_valid_to
      )

)

select * from opened
union all
select * from removed
