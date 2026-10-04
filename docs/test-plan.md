# Test Plan — Lakehouse Data Platform

> Companion to [design.md](design.md). Each test names the mistake it exists to catch: if the design were wrong, this is the test that turns red.
> ✅ = written as code in `code/dbt/`

## Is each record's latest state right?

| # | We check that… | It fails if… | Status |
|:-:|---|---|---|
| 1 | Events arriving **late, out of order or twice** never change the final state: a late older event is ignored, a newer one wins, and 9, 8, 9 in one batch ends at 9 | the guard ordered by time or arrival, or was missing | ✅ 3 dbt unit tests, `_silver_lending.yml` |
| 2 | History holds **each event exactly once** | dedup broke | ✅ `unique` on `event_id` |
| 3 | Every loan's current state is the **highest version in its history** | the guard let an older event win, or a service skipped its version bump | ✅ `assert_current_matches_latest_history.sql` |
| 4 | Running the same batch **twice** changes nothing | a rerun double-applies | |

## Does every paisa match?

| # | We check that… | It fails if… | Status |
|:-:|---|---|---|
| 5 | For every vendor and day, on **both sides**: **matched + breaks = total**, exact to the paisa and in counts | matching dropped or double-counted an item | ✅ `assert_reconciliation_balances.sql` |
| 6 | A missing ₹5 and a duplicate ₹5 show up as **two breaks**, not "balanced" | we compared totals only | |
| 7 | One seeded example per break type lands in **its own class** | a class is missing or mislabelled | |
| 8 | A **cut-off partner file** is held and **deletes nothing** | the snapshot compared any file, so missing rows looked like deletions | ✅ `partner_snapshot_approval.sql` + `assert_partner_snapshot_not_truncated.sql` |
| 9 | An **identical re-sent file** creates no new versions; one changed amount creates exactly one | changes were detected by file arrival, not by row content | |

## Is the data complete and current?

| # | We check that… | It fails if… | Status |
|:-:|---|---|---|
| 10 | Hourly counts match at every hop: Kafka → S3 → Bronze → Silver | a hop dropped or duplicated data | |
| 11 | With one partner file missing, the day is **not published** to finance | the gate ignored the missing file | |
| 12 | Each partner file matches its **control trailer** (rows and paise) | a bad file loaded silently | ✅ rows: `partner_snapshot_approval.sql` |

## Running it

| # | We check that… | It fails if… | Status |
|:-:|---|---|---|
| 13 | Late data in an **open** month restates the day and logs it; in a **closed** month, finance figures stay frozen | a correction silently overwrote a reported number | |
| 14 | A run **killed halfway** leaves no gaps or duplicates after the next run | a step wasn't safe to repeat | |

**Why tests 1 and 5 matter most:** they sit exactly where the two hard guarantees live. If the ordering guard or the reconciliation were wrong, state or money would drift silently, and these turn red first.
