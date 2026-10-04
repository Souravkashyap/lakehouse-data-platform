{{ config(materialized='table') }}
-- Fee rules from the Ops spreadsheet: the rows of the LATEST VALID snapshot.
-- A snapshot is valid only if ALL its rows pass; a bad edit in the sheet never replaces the last good fee rules.
-- Design: section 5 (Ops spreadsheets).
with parsed as (

    select
        product_code,
        fee_type,
        try_to_number(fee_bps, 38, 4)                              as fee_bps,
        -- rupees text to integer paise EXACTLY: decimal arithmetic, never via float
        (try_to_decimal(flat_fee_inr, 38, 2) * 100)::number(38, 0) as flat_fee_paise,
        try_to_date(effective_from)                                as effective_from,
        try_to_date(effective_to)                                  as effective_to,
        flat_fee_inr                                               as flat_fee_raw,
        effective_to                                               as effective_to_raw,
        snapshot_ts
    from {{ source('bronze_sheets', 'fee_rules') }}

),

checked as (

    select
        *,
        coalesce(                  -- a NULL test result counts as a failure, never as a pass
            product_code is not null
            and count(*) over (partition by snapshot_ts, product_code) = 1
            and fee_bps between 0 and 10000
            and (flat_fee_raw is null or flat_fee_paise is not null)
            and effective_from is not null
            and (effective_to_raw is null or effective_to >= effective_from),
            false
        ) as row_ok
    from parsed

),

snapshots as (

    select snapshot_ts, boolean_and(row_ok) as snapshot_ok
    from checked
    group by snapshot_ts

)

select
    c.product_code, c.fee_type, c.fee_bps, c.flat_fee_paise, c.effective_from, c.effective_to,
    c.snapshot_ts as source_snapshot_ts
from checked as c
where c.snapshot_ts = (select max(snapshot_ts) from snapshots where snapshot_ok)
