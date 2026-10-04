# Test Plan — Lakehouse Data Platform

> Companion to [design.md](design.md). For each guarantee: what we assert, and **how the test would fail if the design were wrong**. The two tests marked *real code* are written in full (see `tests/`); the rest are specified here.

| ID | Guarantee | Assertion | Fails if the design were wrong because | Level | Automated? |
|---|---|---|---|---|---|
| T-B-permutation (real code) | Current state = highest sequence | All arrival orders with duplicates give identical state | Ordering used time or arrival order | Unit/property (pytest) | Yes (CI) |
| T-B-replay | Idempotent re-runs | Running a batch twice leaves tables unchanged | The merge double-applies | Integration (dbt) | Yes |
| T-B-late-older | No regression | An older event after a newer one leaves state unchanged | The guard is missing or the `>` wrong | Unit | Yes |
| T-B-dup-batch | Dedup | Duplicates in one batch apply once | Dedup only looks at the target | Unit | Yes |
| T-B-sequence-monotone | Source sequence | Sequence never decreases per entity in Silver | A service did not bump under lock | dbt test | Yes |
| T-C-matched+breaks=total (real code) | No paisa unaccounted for | matched + breaks = total on both sides, exact paise and counts | Matching drops or double-counts rows | SQL test (dbt) | Yes |
| T-C-cancel | Errors do not hide | Missing 500 + duplicate 500 gives two breaks | Totals-only logic | SQL test on seed data | Yes |
| T-C-classes | Every break classified | One seeded case per class lands in its class | A class is missing or mis-assigned | SQL test | Yes |
| T-A-count-audit | Completeness | Counts match Kafka → S3 → Bronze → Silver per hour | A hop drops or duplicates | dbt test | Yes |
| T-A-gate | No partial day published | With one partner file missing, daily Gold excludes the day | Gate ignores the file ledger | Integration | Yes |
| T-A-control-total | File integrity | `loaded + rejected = trailer rows`, paise = trailer total | Bad file loads silently | Load check | Yes |
| T-OPS-restate | Corrections visible | Late data in an open month writes `ops.restatements`; in a closed month leaves `finance_close` unchanged | Silent overwrite | Integration | Planned |
| T-OPS-failure | No partial writes | Kill a run mid-way; next run completes with no gaps or duplicates | Truncate-style staging | Fault injection | Manual first |
| T-P-truncated-snapshot | A cut-off partner file never deletes records | A vendor snapshot with 40% of yesterday's rows fails the guard; `dbt snapshot` does not run; no record is marked REMOVED | The diff ran on any file, so a truncated file looked like millions of deletions | dbt singular test (`assert_partner_snapshot_not_truncated`) gating the snapshot | Yes |
| T-P-diff | Only real changes flow downstream | Re-sending an identical snapshot creates no new versions; one changed amount creates exactly one new version; a key missing from a complete snapshot is invalidated once | The diff used file arrival instead of a row hash, or deletes were applied unguarded | dbt snapshot on seed data | Yes |

**Why these two are written as code:** they sit exactly where the guarantees of the two deep dives live.
- **T-B-permutation:** the ordering guard must give the same final state for every arrival order, with duplicates.
- **T-C-matched+breaks=total:** reconciliation must not lose or double-count a single paisa.

If either were wrong, a reviewer would see money or state silently drift, and these tests turn red first.
