-- Staging view: maps one vendor's FULL daily snapshot into the standard partner shape.
-- Other vendors get their own stg_partner_<unit>_<vendor> model with the same output columns.
-- row_hash covers business columns only (not file metadata), so an identical row in a new file is "unchanged".
-- Design: section 5 (partner files) and section 9 (deep dive C).
with mapped as (

    select
        md5('NBFC-017' || '|' || vendor_txn_id || '|' || upper(line_type)) as partner_line_id,
        'NBFC-017'                        as vendor,
        'lending'                         as business_unit,
        vendor_txn_id                     as vendor_ref,
        our_payment_ref                   as payment_ref,
        our_customer_id                   as customer_id,
        our_loan_id                       as entity_id,
        to_date(txn_date)                 as business_date,
        to_date(snapshot_date)            as snapshot_date,
        upper(line_type)                  as line_type,
        upper(txn_status)                 as vendor_status,
        -- rupees text to integer paise EXACTLY: decimal arithmetic, never via float
        (try_to_decimal(amount_inr, 38, 2) * 100)::number(38, 0) as amount_paise,
        _file_name,
        _file_loaded_at,
        _loaded_at                        as loaded_at
    from {{ source('bronze_partner', 'lending_nbfc_017') }}

)

select
    *,
    -- NULL-safe: concat_ws returns NULL if any input is NULL (fee and tax lines often have no payment_ref),
    -- which would hide real changes from the snapshot, so every column is coalesced first
    md5(concat_ws('|',
        coalesce(vendor_ref, ''), coalesce(line_type, ''), coalesce(payment_ref, ''),
        coalesce(customer_id, ''), coalesce(entity_id, ''), coalesce(business_date::string, ''),
        coalesce(amount_paise::string, ''), coalesce(vendor_status, '')
    )) as row_hash
from mapped
