{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='as_of_date',
    cluster_by=['as_of_date']
) }}
-- cluster_by: verify that clustering expressions are supported on Iceberg tables
-- Periodic snapshot, grain = loan x IST day. Builds exactly ONE date: the run's logical date.
-- Scope: term loans with EMIs (personal and merchant). Revolving credit (BNPL/postpaid) has different SMA rules.
-- We are a lending service provider: the loan sits on the lender's book and repayments go straight to the lender,
-- so payments can arrive late and the lender's classification is official; this table is the operational mirror,
-- reconciled against lender files.
-- Guarantee: DPD and bucket follow the RBI day-end rule as of run_date's day-end in IST. An installment unpaid at
-- day-end of its due date is overdue that day, and an installment is overdue until FULLY paid. DPD counts
-- inclusively: dpd = datediff(day, oldest_unpaid_due_date, as_of_date) + 1 (RBI's example: due 31 Mar, unpaid ->
-- SMA-1 on 30 Apr, SMA-2 on 30 May, NPA on 29 Jun). Buckets: 0 CURRENT, 1-30 SMA_0, 31-60 SMA_1, 61-90 SMA_2, >90 NPA.
-- NPA is sticky until ALL arrears are cleared (not until DPD falls below 91), which needs the previous day's row,
-- so a restated day must be re-run with every later day, in date order (the daily DAG runs one date at a time,
-- max_active_runs=1). An initial load must be backfilled day by day.
-- Retention: daily rows 13 months, month-end rows 5 years (retention job, not in this model). Design: section 7.
{% set run_date = require_run_date() %}

with loans as (

    select *
    from {{ ref('silver_lending_loans_current') }}
    where disbursal_date <= '{{ run_date }}'::date
      and (closed_date is null or closed_date > '{{ run_date }}'::date)
      and (written_off_date is null or written_off_date > '{{ run_date }}'::date)

),

inst as (

    select
        i.*,
        -- the paid date is the IST day: a payment at 00:10 IST does not count for the day before
        coalesce(convert_timezone('UTC', 'Asia/Kolkata', i.fully_paid_at)::date <= '{{ run_date }}'::date, false) as paid_by_day_end
    from {{ ref('silver_lending_installments') }} as i
    inner join loans as l on l.loan_id = i.loan_id

),

per_loan as (

    select
        loan_id,
        sum(iff(not paid_by_day_end, principal_due_paise, 0)) as pos_paise,
        sum(case when due_date <= '{{ run_date }}'::date and not paid_by_day_end then
                case when fully_paid_at is not null then emi_due_paise   -- paid only after run_date: the whole EMI was due on run_date
                     else emi_due_paise - least(paid_paise, emi_due_paise) end
                else 0 end) as overdue_paise,
        count_if(due_date <= '{{ run_date }}'::date and not paid_by_day_end) as installments_overdue,
        min(iff(due_date <= '{{ run_date }}'::date and not paid_by_day_end, due_date, null)) as oldest_overdue_due_date
    from inst
    group by loan_id

),

{% if is_incremental() %}
prev as (

    select loan_id, is_npa, npa_since_date
    from {{ this }}
    where as_of_date = dateadd(day, -1, '{{ run_date }}'::date)

),
{% endif %}

scored as (

    select
        l.*,
        p.pos_paise,
        p.overdue_paise,
        p.installments_overdue,
        p.oldest_overdue_due_date,
        iff(p.oldest_overdue_due_date is null, 0, datediff(day, p.oldest_overdue_due_date, '{{ run_date }}'::date) + 1) as dpd,
        {% if is_incremental() %}
        coalesce(v.is_npa, false)  as prev_is_npa,
        v.npa_since_date           as prev_npa_since_date
        {% else %}
        false                      as prev_is_npa,
        null::date                 as prev_npa_since_date
        {% endif %}
    from loans as l
    inner join per_loan as p on p.loan_id = l.loan_id
    {% if is_incremental() %}
    left join prev as v on v.loan_id = l.loan_id
    {% endif %}

),

flagged as (

    select
        *,
        -- sticky: stays NPA while any installment due on or before the day is still unpaid
        (dpd > 90 or (prev_is_npa and installments_overdue > 0)) as is_npa_today
    from scored

)

select
    loan_id || '|' || '{{ run_date }}'                          as loan_day_key,
    '{{ run_date }}'::date                                      as as_of_date,
    loan_id,
    customer_id,
    lender_id,
    product_code,
    disbursal_date,
    date_trunc(month, disbursal_date)                           as cohort_month,
    datediff(month, disbursal_date, '{{ run_date }}'::date)     as mob,
    principal_paise,
    pos_paise,
    overdue_paise,
    installments_overdue,
    oldest_overdue_due_date,
    dpd,
    case when is_npa_today then 'NPA'
         when dpd = 0 then 'CURRENT'
         when dpd <= 30 then 'SMA_0'
         when dpd <= 60 then 'SMA_1'
         else 'SMA_2' end                                       as dpd_bucket,
    is_npa_today                                                as is_npa,
    case when is_npa_today then iff(prev_is_npa, prev_npa_since_date, '{{ run_date }}'::date) end as npa_since_date,
    sequence                                                    as loan_sequence,
    current_timestamp()                                         as built_at
from flagged
