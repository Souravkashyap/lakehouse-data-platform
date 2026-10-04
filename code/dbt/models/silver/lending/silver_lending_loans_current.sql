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

)
{% else %}
guarded as (

    select * from batch

)
{% endif %}

-- the payload carries the FULL state after each change, so the highest sequence's state
-- IS the current state (no field-level merging)
select
    loan_id,
    sequence,
    status,
    outstanding_principal_paise,
    next_emi_date,
    next_emi_paise,
    days_past_due,
    event_type       as last_event_type,
    event_time_utc   as as_of_event_time,
    loaded_at        as last_loaded_at,
    current_timestamp() as updated_at
from guarded
