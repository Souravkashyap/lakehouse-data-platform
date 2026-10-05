-- docs/test-plan.md test 19: vintage curves never decrease with months on book, and 90+ <= 30+ <= cohort.
-- Returns rows only when a curve fell, or a count exceeded the one it is a subset of. Design: section 7.
with curves as (

    select
        cohort_month, lender_id, product_code, mob,
        cohort_loans, loans_ever_dpd31, loans_ever_dpd91,
        lag(loans_ever_dpd31) over (partition by cohort_month, lender_id, product_code order by mob) as prev_loans_ever_dpd31
    from {{ ref('agg_lending_vintage_monthly') }}

)

select *
from curves
where loans_ever_dpd91 > loans_ever_dpd31
   or loans_ever_dpd31 > cohort_loans
   or loans_ever_dpd31 < prev_loans_ever_dpd31
