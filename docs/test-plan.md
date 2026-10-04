# Test Plan — Lakehouse Data Platform

> Companion to [design.md](design.md). Every test below is written as code in `code/dbt/`. Each one names the mistake it exists to catch: if the design were wrong, this is the test that turns red.

## Is each record's latest state right?

| # | We check that… | It fails if… | Where |
|:-:|---|---|---|
| 1 | Events arriving **late, out of order or twice** never change the final state: a late older event is ignored, a newer one wins, and 9, 8, 9 in one batch ends at 9 | the guard ordered by time or arrival, or was missing | 3 dbt unit tests, `_silver_lending.yml` |
| 2 | History holds **each event exactly once** | dedup broke | `unique` on `event_id` |
| 3 | Every loan's current state is the **highest version in its history** | the guard let an older event win, or a service skipped its version bump | `assert_current_matches_latest_history.sql` |

## Does every paisa match?

| # | We check that… | It fails if… | Where |
|:-:|---|---|---|
| 4 | For every vendor and day, on **both sides**: **matched + breaks = total**, exact to the paisa and in counts | matching dropped or double-counted an item | `assert_reconciliation_balances.sql` |
| 5 | A **cut-off partner file** is held and **deletes nothing** | the snapshot compared any file, so missing rows looked like deletions | `partner_snapshot_approval.sql` + `assert_partner_snapshot_not_truncated.sql` |
| 6 | Each partner file's row count matches its **control trailer** | a bad file loaded silently | `partner_snapshot_approval.sql` |

**Why tests 1 and 4 matter most:** they sit exactly where the two hard guarantees live. If the ordering guard or the reconciliation were wrong, state or money would drift silently, and these turn red first.
