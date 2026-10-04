{% snapshot snap_partner_records %}
{{ config(
    target_schema='silver',
    unique_key='partner_line_id',
    strategy='check',
    check_cols=['row_hash'],
    hard_deletes='invalidate'
) }}
-- hard_deletes='invalidate' needs dbt >= 1.9 (older dbt: invalidate_hard_deletes=True); verify.
-- Unchanged rows (~95%) create no new versions; a changed hash closes the old version and opens a new one;
-- keys missing from the latest APPROVED full file are invalidated (= removed by the partner).
-- Reads only each vendor's latest approved file (partner_snapshot_approval): a truncated or unbalanced file
-- is never compared, so it cannot mark records removed. Design: section 9.

with latest_approved as (

    select vendor, max(snapshot_date) as snapshot_date
    from {{ ref('partner_snapshot_approval') }}
    where is_approved
    group by vendor

)

select s.*
from {{ ref('stg_partner_lending_nbfc_017') }} as s
-- other vendors: union all their staging models (via the dbt ref function) before this join
inner join latest_approved as a
    on a.vendor = s.vendor and a.snapshot_date = s.snapshot_date
-- one file per vendor-day; if a day was re-sent, keep the latest load
qualify dense_rank() over (partition by s.vendor order by s._file_loaded_at desc) = 1

{% endsnapshot %}
