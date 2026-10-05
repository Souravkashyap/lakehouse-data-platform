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
| 13 | A **bad edit in the Ops spreadsheet** (duplicate product, fee out of range, unparseable amount) never replaces the last good fee rules, and the owner is paged | a hand-edited sheet flowed straight into reconciliation | ✅ `ref_fee_rules.sql` + `assert_fee_rules_latest_snapshot_valid.sql` |

## Are the lending numbers right?

| # | We check that… | It fails if… | Status |
|:-:|---|---|---|
| 14 | DPD follows RBI day-end counting: an EMI due 31 Mar and unpaid is SMA-1 on 30 Apr, SMA-2 on 30 May, NPA on 29 Jun; edges 30/31, 60/61, 90/91 | the count was off by one, or buckets used ≥ instead of > | ✅ unit test `dpd_follows_rbi_day_end_rule` |
| 15 | A payment at 00:10 IST on the next day does not count for the day before | the paid date was taken in UTC | ✅ same unit test (loan G) |
| 16 | An NPA loan stays NPA until all arrears are paid, then returns to CURRENT | NPA was recomputed from today's DPD alone | ✅ same unit test (loans H, I) |
| 17 | First-time 30+/90+ dates stay correct after a late payment is recorded | milestones were accumulated from daily history instead of derived from the schedule | |
| 18 | Portfolio totals tie to the loan-level snapshot (loans, POS, overdue) | an aggregate dropped or double-counted loans | ✅ `assert_lending_portfolio_ties_to_loans.sql` |
| 19 | Vintage curves never decrease with months on book, and 90+ ≤ 30+ ≤ cohort | the curve used current DPD, or a cohort's denominator moved | ✅ `assert_vintage_curves_never_decrease.sql` |

## Running it

| # | We check that… | It fails if… | Status |
|:-:|---|---|---|
| 20 | Late data in an **open** month restates the day and logs it; in a **closed** month, finance figures stay frozen | a correction silently overwrote a reported number | |
| 21 | A run **killed halfway** leaves no gaps or duplicates after the next run | a step wasn't safe to repeat | |

**Why tests 1 and 5 matter most:** they sit exactly where the two hard guarantees live. If the ordering guard or the reconciliation were wrong, state or money would drift silently, and these turn red first.
