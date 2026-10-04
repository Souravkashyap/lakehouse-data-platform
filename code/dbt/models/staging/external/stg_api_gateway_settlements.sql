-- Staging view: one row per settlement, flattened out of the raw API pages. Money is integer paise, never float.
-- A re-pulled page replaces the old copy: the newest load of each gateway_txn_id wins.
-- Design: section 5 (third-party APIs).
select
    s.value:gateway_txn_id::string                          as gateway_txn_id,
    s.value:merchant_reference::string                      as payment_ref,
    s.value:settlement_date::date                           as settlement_date,
    try_to_number(s.value:amount_paise::string)             as amount_paise,   -- the API reports paise
    try_to_number(s.value:fee_paise::string)                as fee_paise,
    upper(s.value:status::string)                           as status,
    p._loaded_at                                            as loaded_at
from {{ source('bronze_api', 'gateway_settlements') }} as p,
     lateral flatten(input => p.raw:data) as s
qualify row_number() over (partition by s.value:gateway_txn_id::string order by p._loaded_at desc) = 1
