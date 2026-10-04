{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    incremental_predicates=["DBT_INTERNAL_DEST.loaded_at >= dateadd(day, -8, current_timestamp())"]
) }}
-- Internal side of reconciliation: every money event in one shape, integer paise. Design: section 9.

with lending as (

    select
        event_id                as item_id,
        event_id,
        'lending'               as business_unit,
        vendor,
        -- the business day is the IST day
        to_date(convert_timezone('UTC', 'Asia/Kolkata', event_time_utc)) as business_date,
        case event_type
            when 'LoanDisbursed' then 'DISBURSAL'
            when 'EmiPaid'       then 'EMI_REPAYMENT'
        end                     as movement_type,
        case event_type
            when 'LoanDisbursed' then 'OUT'
            when 'EmiPaid'       then 'IN'
        end                     as direction,
        amount_paise,
        payment_ref,
        customer_id,
        loan_id                 as entity_id,
        event_time_utc,
        loaded_at
    from {{ ref('silver_lending_loan_events') }}
    where event_type in ('LoanDisbursed', 'EmiPaid')
      and amount_paise is not null
    {% if is_incremental() %}
      and loaded_at > (select dateadd(minute, -30, max(loaded_at)) from {{ this }})
    {% endif %}

)

select * from lending
-- insurance and recharge are unioned the same way once their Silver models exist:
-- union all select ... from silver_insurance_policy_events where event_type in ('PremiumPaid', ...)
-- union all select ... from silver_recharge_order_events   where event_type in ('RechargeCompleted', ...)
-- (each via the dbt ref function, added once those Silver models exist)
