{{ config(materialized='view', table_format='default') }}
-- Partner side of reconciliation: the CURRENT version of every partner record, from the SCD2 snapshot.
-- Design: section 9.
select
    partner_line_id,
    business_unit,
    vendor,
    business_date,
    line_type,
    payment_ref,
    vendor_ref,
    customer_id,
    entity_id,
    amount_paise,
    vendor_status,
    snapshot_date,
    row_hash
from {{ ref('snap_partner_records') }}
where dbt_valid_to is null

-- API sources are incremental pulls, not full snapshots, so they don't go through the snapshot diff;
-- they join reconciliation in the same shape.
union all
select
    partner_line_id,
    business_unit,
    vendor,
    business_date,
    line_type,
    payment_ref,
    vendor_ref,
    customer_id,
    entity_id,
    amount_paise,
    vendor_status,
    snapshot_date,
    row_hash
from {{ ref('silver_gateway_settlements') }}
