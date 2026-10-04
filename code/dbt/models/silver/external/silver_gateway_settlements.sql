{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='gateway_txn_id'
) }}
-- Gateway settlements in the SAME shape as silver_partner_records, so they join reconciliation unchanged.
-- Picks up by load time with a 30-minute overlap. Design: section 5 and section 9.
select
    md5('GATEWAY-01' || '|' || gateway_txn_id || '|' || 'TRANSACTION') as partner_line_id,
    'recharge'                       as business_unit,
    'GATEWAY-01'                     as vendor,
    settlement_date                  as business_date,
    'TRANSACTION'                    as line_type,
    payment_ref,
    gateway_txn_id                   as vendor_ref,
    null::string                     as customer_id,
    null::string                     as entity_id,
    amount_paise,
    status                           as vendor_status,
    to_date(loaded_at)               as snapshot_date,
    -- NULL-safe, same column order as the partner staging models
    md5(concat_ws('|',
        coalesce(gateway_txn_id, ''), 'TRANSACTION', coalesce(payment_ref, ''), '', '',
        coalesce(settlement_date::string, ''), coalesce(amount_paise::string, ''), coalesce(status, '')
    ))                               as row_hash,
    gateway_txn_id,
    loaded_at
from {{ ref('stg_api_gateway_settlements') }}
{% if is_incremental() %}
where loaded_at > (select dateadd(minute, -30, max(loaded_at)) from {{ this }})
{% endif %}
