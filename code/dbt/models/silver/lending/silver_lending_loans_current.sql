{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='loan_id'
) }}
-- THE ORDERING GUARD. Guarantee: current state of a loan is the event with the highest source sequence
-- seen so far, whatever the arrival order, duplicates or replays. Design: section 8.

with batch as (

    select * from {{ ref('silver_lending_loan_events') }}
    {% if is_incremental() %}
    where loaded_at > (select dateadd(minute, -30, max(last_loaded_at)) from {{ this }})
    {% endif %}
    -- highest sequence per loan inside the batch
    qualify row_number() over (partition by loan_id order by sequence desc) = 1

),

{% if is_incremental() %}
guarded as (

    -- a late or older event can never overwrite newer state; replays and duplicates change nothing
    select b.*
    from batch as b
    left join {{ this }} as t on t.loan_id = b.loan_id
    where t.loan_id is null or b.sequence > t.sequence

),
{% else %}
guarded as (

    select * from batch

),
{% endif %}

-- the payload carries the FULL state after each change, so the highest sequence's state
-- IS the current state (no field-level merging).
-- DPD is NOT taken from the source state: an unpaid loan emits no events, so a stored DPD goes stale
-- exactly when it matters. DPD is computed as of a date in Gold.
parsed as (

    select *, parse_json(state_json) as s
    from guarded

)

select
    loan_id,
    sequence,
    s:application_id::string                                       as application_id,
    customer_id,
    lender_id,
    s:product_code::string                                         as product_code,
    s:status::string                                               as status,
    try_to_number(s:principal_paise::string)                       as principal_paise,
    try_to_number(s:processing_fee_paise::string)                  as processing_fee_paise,
    try_to_number(s:net_disbursed_paise::string)                   as net_disbursed_paise,
    try_to_number(s:interest_rate_bps::string)                     as interest_rate_bps,
    try_to_number(s:apr_bps::string)                               as apr_bps,
    try_to_number(s:tenure_months::string)                         as tenure_months,
    try_to_number(s:emi_paise::string)                             as emi_paise,
    try_to_number(s:outstanding_principal_paise::string)           as outstanding_principal_paise,
    try_to_number(s:next_emi_paise::string)                        as next_emi_paise,
    s:next_emi_date::date                                          as next_emi_date,
    s:first_emi_date::date                                         as first_emi_date,
    s:maturity_date::date                                          as maturity_date,
    convert_timezone('UTC', s:disbursed_at::timestamp_tz)::timestamp_ntz    as disbursed_at,
    convert_timezone('UTC', s:closed_at::timestamp_tz)::timestamp_ntz       as closed_at,
    convert_timezone('UTC', s:written_off_at::timestamp_tz)::timestamp_ntz  as written_off_at,
    convert_timezone('Asia/Kolkata', s:disbursed_at::timestamp_tz)::date    as disbursal_date,
    -- closed_date covers repaid, foreclosed and cancelled loans
    convert_timezone('Asia/Kolkata', s:closed_at::timestamp_tz)::date       as closed_date,
    convert_timezone('Asia/Kolkata', s:written_off_at::timestamp_tz)::date  as written_off_date,
    try_to_number(s:schedule_version::string)                      as schedule_version,
    to_json(s:installments)                                        as installments_json,
    event_type       as last_event_type,
    event_time_utc   as as_of_event_time,
    loaded_at        as last_loaded_at,
    current_timestamp() as updated_at
from parsed
