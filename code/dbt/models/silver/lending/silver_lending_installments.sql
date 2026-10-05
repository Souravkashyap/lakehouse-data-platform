{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='loan_id'
) }}
-- One row per loan and installment, taken from the loan's guarded latest state. A loan touched in this run
-- has ALL its installments replaced (a restructured schedule can shrink), so no ordering logic is needed here:
-- the ordering guard already ran in silver_lending_loans_current. Design: section 8.

with touched as (

    select * from {{ ref('silver_lending_loans_current') }}
    {% if is_incremental() %}
    where last_loaded_at > (select dateadd(minute, -30, max(loan_loaded_at)) from {{ this }})
    {% endif %}

)

select
    t.loan_id,
    try_to_number(i.value:installment_no::string)      as installment_no,
    i.value:due_date::date                              as due_date,
    try_to_number(i.value:principal_due_paise::string) as principal_due_paise,
    try_to_number(i.value:interest_due_paise::string)  as interest_due_paise,
    try_to_number(i.value:emi_due_paise::string)       as emi_due_paise,
    coalesce(try_to_number(i.value:paid_paise::string), 0) as paid_paise,
    convert_timezone('UTC', i.value:fully_paid_at::timestamp_tz)::timestamp_ntz as fully_paid_at,
    coalesce(try_to_number(i.value:bounce_count::string), 0) as bounce_count,
    i.value:last_bounce_reason::string                  as last_bounce_reason,
    t.customer_id,
    t.lender_id,
    t.product_code,
    t.status as loan_status,
    t.disbursal_date,
    t.sequence as loan_sequence,
    t.last_loaded_at as loan_loaded_at
from touched as t,
     lateral flatten(input => parse_json(t.installments_json)) as i
