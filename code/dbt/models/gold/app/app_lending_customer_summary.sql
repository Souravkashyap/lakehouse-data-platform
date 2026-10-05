{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id'
) }}
-- What the app shows a customer about their loans (15-minute build). Recomputed from ALL loans of every
-- customer touched since the last run.
-- No clock in this model: the app shows "due" or "N days overdue" by comparing earliest_unpaid_due_date with
-- today at display time, so the row only changes when the customer's loans change.
-- Feeds gold_app.app_customer_profile -> DynamoDB push. Design: section 7.

with touched as (

    select distinct customer_id
    from {{ ref('silver_lending_loans_current') }}
    {% if is_incremental() %}
    where last_loaded_at > (select dateadd(minute, -30, max(source_loaded_at)) from {{ this }})
    {% endif %}

),

loans as (

    select l.*
    from {{ ref('silver_lending_loans_current') }} as l
    inner join touched as t on t.customer_id = l.customer_id

),

per_loan as (

    select
        customer_id,
        count_if(status = 'ACTIVE')                                         as active_loans,
        sum(iff(status = 'ACTIVE', outstanding_principal_paise, 0))         as total_outstanding_principal_paise,
        max(last_loaded_at)                                                 as source_loaded_at
    from loans
    group by customer_id

),

unpaid as (

    select
        l.customer_id,
        i.due_date,
        i.emi_due_paise - least(i.paid_paise, i.emi_due_paise) as remaining_paise
    from {{ ref('silver_lending_installments') }} as i
    inner join loans as l on l.loan_id = i.loan_id
    where l.status = 'ACTIVE'
      and i.fully_paid_at is null

),

on_earliest as (

    -- keep only the unpaid installments due on each customer's earliest unpaid due date
    select customer_id, due_date, remaining_paise
    from unpaid
    qualify due_date = min(due_date) over (partition by customer_id)

),

earliest as (

    select
        customer_id,
        min(due_date)        as earliest_unpaid_due_date,
        sum(remaining_paise) as amount_due_on_earliest_paise
    from on_earliest
    group by customer_id

)

select
    p.customer_id,
    p.active_loans,
    p.total_outstanding_principal_paise,
    e.earliest_unpaid_due_date,
    e.amount_due_on_earliest_paise,
    p.source_loaded_at,
    current_timestamp() as updated_at
from per_loan as p
left join earliest as e on e.customer_id = p.customer_id
