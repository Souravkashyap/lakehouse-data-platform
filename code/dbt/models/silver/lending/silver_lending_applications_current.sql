{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='application_id'
) }}
-- Ordering guard for applications. Guarantee: current state of an application is the event with the highest
-- source sequence seen so far, whatever the arrival order, duplicates or replays. Design: section 8.
-- Applications need no separate history table: raw history stays in S3 for 5 years and in Bronze for
-- 90 days, and the guard makes duplicates and replays harmless.

with batch as (

    select * from {{ ref('stg_lending_events') }}
    where topic = 'lending.loan_application.events'
    {% if is_incremental() %}
      and loaded_at > (select dateadd(minute, -30, max(last_loaded_at)) from {{ this }})
    {% endif %}
    -- highest sequence per application inside the batch
    qualify row_number() over (partition by aggregate_id order by sequence desc, loaded_at desc) = 1

),

{% if is_incremental() %}
guarded as (

    -- a late or older event can never overwrite newer state; replays and duplicates change nothing
    select b.*
    from batch as b
    left join {{ this }} as t on t.application_id = b.aggregate_id
    where t.application_id is null or b.sequence > t.sequence

),
{% else %}
guarded as (

    select * from batch

),
{% endif %}

parsed as (

    select *, parse_json(state_json) as s
    from guarded

)

select
    aggregate_id                                                   as application_id,
    sequence,
    customer_id,
    s:product_code::string                                         as product_code,
    s:channel::string                                              as channel,
    lender_id,
    s:status::string                                               as status,
    try_to_number(s:requested_amount_paise::string)                as requested_amount_paise,
    try_to_number(s:requested_tenure_months::string)               as requested_tenure_months,
    try_to_number(s:approved_amount_paise::string)                 as approved_amount_paise,
    try_to_number(s:approved_tenure_months::string)                as approved_tenure_months,
    try_to_number(s:approved_rate_bps::string)                     as approved_rate_bps,
    s:rejection_reason::string                                     as rejection_reason,
    convert_timezone('UTC', s:submitted_at::timestamp_tz)::timestamp_ntz as submitted_at,
    convert_timezone('UTC', s:decided_at::timestamp_tz)::timestamp_ntz   as decided_at,
    convert_timezone('UTC', s:accepted_at::timestamp_tz)::timestamp_ntz  as accepted_at,
    convert_timezone('UTC', s:disbursed_at::timestamp_tz)::timestamp_ntz as disbursed_at,
    convert_timezone('UTC', s:closed_at::timestamp_tz)::timestamp_ntz    as closed_at,
    convert_timezone('Asia/Kolkata', s:submitted_at::timestamp_tz)::date as submitted_date,
    s:loan_id::string                                              as loan_id,
    event_type                                                     as last_event_type,
    loaded_at                                                      as last_loaded_at,
    current_timestamp()                                            as updated_at
from parsed
