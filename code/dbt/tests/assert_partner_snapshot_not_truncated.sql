-- docs/test-plan.md test 5. Fails (pages vendor ops) when a vendor's newest loaded file was NOT approved:
-- its control total is missing or disagrees, or its row count is outside 90-110% of the previous file.
-- The file is held automatically (the snapshot reads only approved files); this test makes it visible.
-- Design: section 5.
with newest as (

    select *
    from {{ ref('partner_snapshot_approval') }}
    qualify row_number() over (partition by vendor order by snapshot_date desc) = 1

)

select vendor, snapshot_date, row_count, previous_rows, trailer_rows
from newest
where not is_approved
