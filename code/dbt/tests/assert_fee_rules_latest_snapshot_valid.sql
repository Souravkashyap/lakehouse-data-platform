-- Returns the newest fee-rules snapshot_ts if it is NOT the one ref_fee_rules is serving, i.e. it failed validation.
-- Pages the sheet owner; the reference table keeps the last good version until the sheet is fixed.
select max(snapshot_ts) as bad_snapshot_ts
from {{ source('bronze_sheets', 'fee_rules') }}
-- coalesce: with no valid snapshot at all, ref_fee_rules is empty and the test must still fail
having max(snapshot_ts) > (select coalesce(max(source_snapshot_ts), '') from {{ ref('ref_fee_rules') }})
