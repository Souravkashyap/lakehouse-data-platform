{{ config(
    materialized='incremental',
    incremental_strategy='append',
    cluster_by=['to_date(loaded_at)']
) }}
-- cluster_by: verify that clustering expressions are supported on Iceberg tables
-- Guarantee: append-only history, one row per event_id. A duplicate arriving within 8 days is skipped,
-- never re-written, so history stays immutable. Residual risk: a duplicate more than 8 days late is
-- caught by reconciliation (section 9). Design: section 8.

with source_rows as (

    select * from {{ ref('stg_lending_events') }}
    where topic in ('lending.loan.events')
    {% if is_incremental() %}
    -- select new rows by ARRIVAL (load time), never by event time; the 30-minute overlap
    -- is harmless because already-stored event_ids are skipped below
      and loaded_at > (select dateadd(minute, -30, max(loaded_at)) from {{ this }})
    {% endif %}

),

deduped as (

    -- one row per event_id inside the batch (sink retries can deliver the same event twice)
    select * from source_rows
    qualify row_number() over (partition by event_id order by loaded_at, kafka_offset) = 1

)

select
    d.event_id,
    d.event_type,
    d.aggregate_id as loan_id,
    d.sequence,
    d.event_time_utc,
    d.loaded_at,
    d.customer_id,
    d.lender_id,
    d.amount_paise,
    d.payment_ref,
    d.installment_no,
    d.data_json,
    d.state_json,
    d.topic,
    d.kafka_partition,
    d.kafka_offset
from deduped as d
{% if is_incremental() %}
-- skip events already stored; only the last 8 days are scanned (clustered by load date)
where not exists (
    select 1 from {{ this }} as t
    where t.event_id = d.event_id
      and t.loaded_at >= dateadd(day, -8, current_timestamp())
)
{% endif %}
