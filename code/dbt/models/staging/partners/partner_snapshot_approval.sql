-- Truncation guard as DATA, not just a test. One row per vendor and snapshot_date, approved only if the
-- file matches its control trailer and has 90-110% of the previous file's rows. The snapshot reads ONLY the
-- latest approved file per vendor, so a truncated file can never mark records "removed", whichever DAG runs
-- the snapshot. Design: section 5 (partner files).
with counts as (

    select vendor, snapshot_date, count(*) as row_count
    from {{ ref('stg_partner_lending_nbfc_017') }}
    group by vendor, snapshot_date
    -- other vendors: union all their counts the same way

),

with_previous as (

    select
        c.*,
        lag(c.row_count) over (partition by c.vendor order by c.snapshot_date) as previous_rows,
        t.trailer_rows
    from counts as c
    left join {{ source('bronze_partner', 'lending_nbfc_017_control') }} as t
        on t.vendor = c.vendor and to_date(t.snapshot_date) = c.snapshot_date

)

select
    vendor,
    snapshot_date,
    row_count,
    previous_rows,
    trailer_rows,
    (
        trailer_rows is not null
        and trailer_rows = row_count
        and (previous_rows is null or row_count between 0.9 * previous_rows and 1.1 * previous_rows)
    ) as is_approved
from with_previous
