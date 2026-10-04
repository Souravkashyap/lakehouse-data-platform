-- docs/test-plan.md test 3. Returns loans whose current sequence is not the highest sequence in history;
-- any row means the ordering guard failed. Design: section 8.
with history as (

    select loan_id, max(sequence) as max_sequence
    from {{ ref('silver_lending_loan_events') }}
    group by loan_id

)

select c.loan_id, c.sequence as current_sequence, h.max_sequence
from {{ ref('silver_lending_loans_current') }} as c
inner join history as h on h.loan_id = c.loan_id
where c.sequence <> h.max_sequence
