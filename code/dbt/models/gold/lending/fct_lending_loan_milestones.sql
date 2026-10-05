{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='loan_id'
) }}
-- Accumulating snapshot, one row per loan: the first date it reached DPD 1 / 31 / 61 / 91.
-- Guarantee: these dates are a pure function of due dates and full-payment dates, so they are recomputed from
-- the current schedule whenever the loan changes. They stay correct after late or reversed payments, with no
-- scan of daily history. Time passing alone never changes a row.
-- A date later than today is a projection (the loan reaches that milestone on that date unless paid), so
-- consumers MUST filter `<= as-of date`.
-- Derivation: DPD reaches 31 at the day-end of due + 30 if that installment is still unpaid then; the earliest
-- such day over all installments is the first day DPD >= 31. Same for 61 (due + 60) and 91 (due + 90).
-- fpd30 = the FIRST installment is still unpaid 30 days after its due date. Design: section 7.

with touched as (

    select * from {{ ref('silver_lending_loans_current') }}
    {% if is_incremental() %}
    where last_loaded_at > (select dateadd(minute, -30, max(loan_loaded_at)) from {{ this }})
    {% endif %}

),

inst as (

    select
        i.loan_id,
        i.installment_no,
        i.due_date,
        convert_timezone('UTC', 'Asia/Kolkata', i.fully_paid_at)::date as paid_date
    from {{ ref('silver_lending_installments') }} as i
    inner join touched as t on t.loan_id = i.loan_id

),

per_loan as (

    select
        loan_id,
        min(due_date)                                                                                          as first_due_date,
        min(iff(paid_date is null or paid_date > due_date, due_date, null))                                    as first_dpd1_date,
        min(iff(paid_date is null or paid_date > dateadd(day, 30, due_date), dateadd(day, 30, due_date), null)) as first_dpd31_date,
        min(iff(paid_date is null or paid_date > dateadd(day, 60, due_date), dateadd(day, 60, due_date), null)) as first_dpd61_date,
        min(iff(paid_date is null or paid_date > dateadd(day, 90, due_date), dateadd(day, 90, due_date), null)) as first_dpd91_date,
        max(iff(installment_no = 1 and (paid_date is null or paid_date > dateadd(day, 30, due_date)), dateadd(day, 30, due_date), null)) as fpd30_date
    from inst
    group by loan_id

)

select
    t.loan_id,
    t.customer_id,
    t.lender_id,
    t.product_code,
    date_trunc(month, t.disbursal_date)  as cohort_month,
    t.disbursal_date,
    t.principal_paise,
    t.status,
    t.closed_date,
    t.written_off_date,
    (t.status = 'CANCELLED')             as is_cancelled_cooling_off,
    p.first_due_date,
    p.first_dpd1_date,
    p.first_dpd31_date,
    p.first_dpd61_date,
    p.first_dpd91_date,
    p.fpd30_date,
    t.last_loaded_at                     as loan_loaded_at
from touched as t
left join per_loan as p on p.loan_id = t.loan_id
