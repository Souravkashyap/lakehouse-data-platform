-- Staging view over both lending topics (loan and loan application): raw rename, cast and JSON extraction.
-- Money is integer paise, never float.
-- Payload: {customer_id, data{this change}, state{FULL entity state after the change}}.
-- The payload is kept as JSON text (data_json, state_json) because Silver and Gold are Iceberg tables and
-- VARIANT support on Iceberg must be verified; JSON text is readable by any engine.
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
    payload:state:lender_id::string                           as lender_id,
    try_to_number(payload:data:amount_paise::string)          as amount_paise,
    payload:data:payment_ref::string                          as payment_ref,
    try_to_number(payload:data:installment_no::string)        as installment_no,
    to_json(payload:data)                                     as data_json,
    to_json(payload:state)                                    as state_json
from {{ source('bronze', 'lending_events') }}
