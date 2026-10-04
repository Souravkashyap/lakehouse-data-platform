-- Staging view: raw rename, cast and JSON extraction. Money is integer paise, never float.
-- Payload: {customer_id, data{amount_paise, payment_ref, lender_id}, state{FULL entity state after the change}}.
-- Design: section 8 (deep dive B).
select
    topic,
    kafka_partition,
    kafka_offset,
    event_id,
    event_type,
    aggregate_id,
    sequence::number(38, 0)                                   as sequence,
    convert_timezone('UTC', occurred_at)::timestamp_ntz       as event_time_utc,
    _loaded_at                                                as loaded_at,
    payload:customer_id::string                               as customer_id,
    try_to_number(payload:data:amount_paise::string)          as amount_paise,
    payload:data:payment_ref::string                          as payment_ref,
    payload:data:lender_id::string                            as vendor,
    payload:state:status::string                              as status,
    try_to_number(payload:state:outstanding_principal_paise::string) as outstanding_principal_paise,
    payload:state:next_emi_date::date                         as next_emi_date,
    try_to_number(payload:state:next_emi_paise::string)       as next_emi_paise,
    try_to_number(payload:state:days_past_due::string)        as days_past_due
from {{ source('bronze', 'lending_events') }}
