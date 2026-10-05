# Lakehouse Data Platform for Lending, Insurance and Recharge — Design Doc

**Author:** Sourav Kashyap · **Status:** Draft for review · **Updated:** 5 Oct 2026 · **Code:** `code/` · **Tests:** `docs/test-plan.md`

## 0. Summary

We build one data platform for lending, insurance and recharge. Each business writes its changes as events in the same database transaction as the change itself, so nothing is lost or invented. Events flow through Kafka into an S3 data lake. Snowflake cleans them into tables for analysts, finance, data scientists, auditors and customer apps. Raw data is kept 5 years, so any number can be rebuilt and traced.

- **Correct latest state (§8, code).** Each loan, policy and order ends in its right state, however events are duplicated, late or out of order.
- **Every paisa matches (§9, code).** Our books are proven against partner records, or every difference is listed and owned.
- **Completeness rule (§6, design only).** A table is published as final only when the day's data is shown to be complete.

## 1. Context & assumptions

**From the brief:** five source types (service databases, event streams, partner files, third-party APIs, Ops spreadsheets); ~10k events/s at peak and ~500M file rows/day; 50M customers; 5 years of history; money in integer paise. No schemas, event types, file formats or latency targets are given, so the items below are **our assumptions**.

**Key assumptions**

| # | Assumption | If wrong |
|---|---|---|
| A1 | Peak 10k events/s on the busiest days; average ~2,000/s ≈ 173M events/day. | At 10k/s average (864M/day), cost ≈ 5×. |
| A5 | Partners send a **full daily snapshot** (~500M rows, ~30–50 vendors, by 02:00 IST); ~5% changes daily. | New-rows-only files make the diff step (§5) unnecessary. |
| A6 | Partner lines carry our `payment_ref`. | The weaker secondary match does most of the work (§9). |
| A8 | SLAs: apps ≤ 1 h; analysts by 07:00 IST; data scientists 06:00; finance reconciled 09:00. | Seconds-level freshness needs streaming. |
| A15 | Lending: we are a Lending Service Provider; loans sit on lenders' books (~10M active, EMIs), so the lender's classification is official. | Revolving credit (BNPL) needs a different model. |

**Other assumptions**
- **A7.** One customer ID across businesses; no identity resolution.
- **A11.** AWS Mumbai (`ap-south-1`); payment data stays in India (RBI).
- **A13.** Services publish business events via the outbox with one envelope (`event_id`, entity ID, `sequence`, `occurred_at`).
- **A14.** Business teams own Gold grains, retention and metric definitions through a short data contract.

## 2. Requirements, SLAs and non-goals

- **Must do**
  - Land all sources in a lake; build Silver and Gold in an open table format in our own S3.
  - Keep 5 years: raw, finance, money and audit tables 5 years; other Silver and Gold per business-team requirement, 2–5 years.
  - Show completeness and freshness per table; meet A8; reproduce any reported number.
- **Must not do:** sit on a transaction's critical path; change a closed month silently; match money on "close" amounts.
- **Non-goals:** real-time fraud or online ML; identity resolution (A7); multi-region DR (A11); clickstream; a full privacy design (erasure versus 5-year history is noted, not designed; tokenisation plus crypto-shredding is the likely route).

## 3. Napkin math

**Inputs:** 2,000 events/s average (10k peak, A1) · ~1 KB per event · 500M partner rows/day of ~300 B (A5) · Parquet ≈ 5× smaller than raw · Snowflake ≈ $3/credit.

**Volume and storage**

| What | Estimate | How |
|---|---|---|
| Events | ≈ 173M/day (max 864M) | 2,000/s × 86,400 s |
| All records | ≈ 673M/day | + 500M partner rows |
| Peak ingest | ≈ 10 MB/s (30 MB/s with 3 replicas) | 10,000/s × 1 KB |
| Landed per day | ≈ 65 GB (≈ 37 GB kept long-term) | events 35 GB + partner snapshot 30 GB; only partner changes (5% ≈ 1.5 GB) are kept |
| S3 raw, 5 years | ≈ 71 TB | events 35 GB × 1,825 ≈ 64 TB + partner ≈ 7 TB (90 days of snapshots, then month-ends and changes) |
| Kafka, 7 days | ≈ 3.6 TB | 2 MB/s × 7 days × 3 replicas |
| Snowflake Bronze, 90 days | ≈ 3.4 TB | events 3.2 TB + partner 0.2 TB |
| Silver + Gold (Iceberg) | ≈ 15 TB year 1 → ≈ 73 TB year 5 | ~40 GB/day, if every team keeps 5 years |

**Compute:** ≈ 52 Snowflake credits/day ≈ $4.7k/month: BI 32 (Medium, ~8 h/day; ≈ 60%) · transform 13.6 · load 4.8 (96 COPY runs on Small ≈ $430/month) · finance 2. The nightly partner load and diff (150 GB, 500M rows) adds 6–12 credits/day on a Large warehouse, ~45–90 min (estimate; needs one test load).

**Monthly cost** ($)

| Line | Year 1 | Year 5 |
|---|---|---|
| Snowflake compute | 4,700 | 4,700 |
| Partner load + diff | 540–1,080 | 540–1,080 |
| Kafka (MSK) | ~820 | ~820 |
| Kafka Connect (Debezium + S3 sink) | ~1,100 | ~1,100 |
| Airflow, SFTP, cross-zone traffic | ~820 (360 + 260 + 200) | ~820 |
| DynamoDB (reads 160–650 + writes 560 + storage 31) | 750–1,250 | 750–1,250 |
| Storage: Snowflake Bronze + S3 raw + Silver/Gold | 85–135 + 280 + 370 | 85–135 + 490–545 + 1,800 |
| **Total** | **≈ 9.5–10.6k** | **≈ 11–12.3k** |

*List prices, verify for Mumbai:* MSK broker ~$0.21/h and storage ~$0.10/GB-month · Connect ~$0.11/unit-hour (~14 units) · MWAA ~$0.49/h · Transfer Family ~$0.30/h · cross-zone ~$0.02/GB (~10 TB/month) · S3 and Iceberg storage ~$25/TB-month (S3 raw tiered).

**What it tells us**
- Compute is about half the bill, and BI is 60% of it: auto-suspend, result caching and scheduled refresh are the levers (§10).
- Storage is the line that grows: keeping partner **changes** instead of full daily snapshots saves ≈ 48 TB over 5 years; dropping the Silver payload after 90 days (§10) is the next lever.

## 4. Architecture

![Architecture of the lakehouse data platform](architecture.svg)

1. Each service writes its change and an outbox row in one transaction. Debezium (log-based CDC) publishes it to Kafka on MSK. Partner files, APIs and spreadsheets land in S3.
2. A Kafka Connect sink writes events to S3 as raw Bronze: kept 5 years, never edited.
3. Every 15 minutes `COPY INTO` loads new S3 files into Snowflake Bronze (native, 90 days).
4. dbt builds Silver (deduplicated, ordered, Iceberg in our S3), then Gold per consumer.
5. Gold feeds analysts, finance and data scientists; changed rows go to DynamoDB for apps. Auditors read Silver, `finance_close` and raw S3.
6. Airflow runs everything from COPY onwards. OpenMetadata shows lineage from the dbt manifest.

**Decisions** (where each breaks first: §11)

| Component | Chose | Rejected | Why | Cost |
|---|---|---|---|---|
| Event capture | Outbox + Debezium; direct table CDC only as raw fallback for legacy services | Polling `updated_at`; triggers; services writing to Kafka; waiting for every team | Event and change commit together; fallback flows from day one | Service teams change code; fallback exposes internal schemas (money always via outbox) |
| Transport | Kafka on MSK, RF 3, 7 days, Avro + schema registry; one topic per entity per unit, key = entity ID (6 topics, 57 partitions) | Kinesis, Pulsar, Confluent Cloud; topic per event type | Native to Debezium; managed; 7 days covers a long weekend; per-entity order | Always-on cluster; ≈ 3.6 TB; ~15–20 support topics |
| Raw landing | Kafka Connect S3 sink, Parquet, 5-min rotation | Spark job writing Delta; Firehose | Cheapest write-once copy; retries overwrite | Duplicates (removed in Silver); small files (≈ 2 MB) |
| Load | `COPY INTO` every 15 min, Small, today + yesterday | Snowpipe (≈ $90/month fees at 16,416 files/day plus serverless, verify; no gain as dbt runs every 15 min) | COPY skips loaded files; wide windows are safe | Up to 15 min added to freshness |
| Bronze | S3 raw 5 years + Snowflake 90 days | "Latest batch only" (loses rows if dbt fails after COPY); 5 years in Snowflake (≈ $1.6–2.6k/month extra) | S3 holds 5 years; 90 days covers most rebuilds | Daily delete job; older rebuilds need a runbook |
| Processing | dbt on Snowflake, Airflow (MWAA), DAG per 15 min, dbt per unit | Spark/Delta on Databricks; Streams + Tasks (no tests or docs); Dynamic Tables (full-recompute risk) | One place to clean, one lineage, team skills | We own incremental logic and Airflow |
| Lakehouse tables | Silver and Gold as Snowflake-managed Iceberg in our S3; Bronze native | All-native; Bronze as Iceberg; external catalog (Glue/Polaris) | Open format, no lock-in | No Fail-safe (verify): rebuild from S3; no S3 lifecycle tiering |
| Apps store | DynamoDB + API | Snowflake (24×7 warehouse ≈ $2.2k/month, 100 ms–s latency); Redis/MemoryDB (≈ $4–5k/month); Aerospike | 125 GB, single-key reads, single-digit ms; cheapest | Second store to sync; writes ≈ $560/month |

## 5. Getting data in

- **Service DBs.** Outbox row in the business transaction → Debezium reads the log → Kafka → S3 sink → COPY.
  - Debezium or Kafka down: the log and Kafka (7 days) hold the events.
  - A bad event hits only its unit's topic; heartbeats expose a dead connector.
- **Legacy services.** Debezium table CDC, raw only, LSN as sequence. A table change breaks only that fallback model.
- **Partner files.** SFTP or S3 upload to `landing/<unit>/<vendor>/snapshot_date=D/` (versioning on).
  - Ledger row with checksum; load once the control file is present and size is stable; same checksum is skipped.
  - Nightly COPY of the **full snapshot** (Large) → per-vendor Bronze (last 2 snapshots) → mapping config + row hash.
  - **Truncation guard:** row count within ±10% of yesterday, control totals present. A failing snapshot is **held and no records are marked removed**; a cut-off file would look like millions of deletions.
  - **Daily diff vs yesterday (dbt snapshot):** new key = NEW, changed hash = CHANGED, missing key = REMOVED; unchanged rows (~95%) do nothing. Output: `silver.partner_records` (current versions) + `silver.partner_record_changes`.
  - A changed record for a past business date is a partner restatement: that date is re-reconciled (§9).
  - `loaded + rejected ≠ trailer rows` or paise total ≠ trailer total: file held. Over 1% rows rejected: quarantined, partner manager alerted. Missing file: page at 04:30 (§6).
- **Third-party APIs** (example: payment gateway settlements, for recharge).
  - Daily Airflow pull at 01:30 for day D; cursor pagination; each raw page stored untouched under a fixed key (`ingest_date=D/page_NNNNN.json`).
  - COPY into VARIANT Bronze → one row per settlement (exact paise, newest copy wins) → `silver_gateway_settlements` in the **same shape as partner records**, so it joins reconciliation unchanged. Incremental feed: no snapshot diff.
  - 429: wait for `Retry-After`; 5xx: exponential backoff, then task retry. A rerun overwrites the same keys, so it is idempotent. A missing day shows as missing-at-partner breaks (§9).
- **Ops spreadsheets** (example: fee rules).
  - Each daily run exports the **whole sheet** to S3 as a CSV snapshot (every pull kept) → text-only Bronze → `ref_fee_rules`.
  - Every row is checked (unique product code, fee 0–10,000 bps, rupees → exact paise, valid date range); a snapshot is used only if **all** rows pass.
  - A bad edit never replaces good rules: the last valid snapshot keeps serving and a test pages the sheet owner.

**Code per source:** `code/sources/service_db/outbox.sql`; `code/connect/`; `lakehouse_partner_daily.py` + partner models; `lakehouse_external_daily.py` + `staging/external/` (Appendix C). Topics, envelope, payloads, S3 layout (A13): Appendix A.

**Schema changes**
- Avro schemas with backward compatibility are enforced in the registry: a breaking change is rejected before Kafka.
- New optional fields flow through in the raw payload and are mapped into Silver when a team needs them.
- Vendor format changes are caught by the per-vendor header check and fixed in that vendor's mapping config.

**Completeness of loading**
- Every COPY result is checked; errored files go to quarantine.
- A file ledger compares the S3 listing with Snowflake load history; a file unloaded after 30 min alerts.
- A per-partition Kafka offset-continuity check proves nothing was lost to Snowflake (whether producer transactions leave offset gaps: verify).

## 6. Making it trustworthy

- **Correct.** See §8 (record state) and §9 (money).
- **Complete** (hard problem A, design only). Two tiers:
  - **Intraday** (apps, intraday analysts): non-blocking. We publish what has arrived; every table carries `data_as_of`. Worst case ≈ 33 min (sink 5 + COPY wait 15 + load 2 + Silver 3 + app Gold 3 + push 5).
  - **Daily** (data scientists 06:00, analysts 07:00, finance 09:00): a blocking gate. An arrival hour is complete when Kafka offsets reach Silver with no gaps, counts match across Kafka, S3, Bronze and Silver, and Debezium heartbeats show live connectors.
  - Business day D is complete when arrival hours through D+1 01:00 IST are complete (1 h allowed lateness, tuned to p99.9 of arrival minus event time) and all expected partner files for D are loaded, match control totals and are diffed. Result: `ops.completeness`.
  - The gate is **split in two** because partner snapshots are full files. **Internal gate** (≈ 01:30): releases features and daily analytics, done ≈ 02:30. **Partner gate** (loaded and diffed, ≈ 03:00–03:40): releases finance reconciliation, done ≈ 04:40.
  - *Guarantee:* a business day appears in finance Gold only when every unit's events and every expected partner file for that day are loaded with no offset gaps and matching counts at every hop; if late data changes a published, non-closed day, the day is restated and the change is logged.
  - *Escalation:* page at 04:30 if the gate is closed. At 06:00 and 07:00 the day is marked provisional, naming the missing source. Finance never gets an incomplete day as final.
  - Late data in an open month restates day D in the next run (`ops.restatements`). In a closed month, `finance_close` is unchanged and the change is a prior-period adjustment in the current month.
  - *Decision sentence:* we chose a completeness gate with allowed lateness over a fixed-time cutoff and over freshness-only checks, because a fixed cutoff publishes incomplete days silently and "recent" does not mean "whole"; it costs a late publish when a source is late (finance sees "unreconciled, partner X pending", not a wrong figure); it breaks first when one chronically late partner blocks the gate every night. Source-emitted "hour closed" markers are a stronger upgrade but need every service changed.
- **Fresh.** Intraday ≤ 1 h (worst ≈ 33 min), daily per A8; `data_as_of` on every table; `dbt source freshness` on Bronze `_loaded_at`.
- **Traceable.** `ops.run_manifest`, lineage (§10); raw S3 lets any number be rebuilt.
- **Means what they think it means.** Metrics are defined once in dbt, exposed as Snowflake semantic views for BI and Cortex Analyst (plain-English questions). Open Semantic Interchange (OSI) is the portability path (maturity and Snowflake status: verify). It risks wrong answers on money; guardrails limit but do not remove it:
  - Only certified Gold tables and approved metrics; every answer shows its SQL; all questions logged.
  - Official finance figures come only from `gold_finance` and `finance_close`.
  - ~30–50 known questions run as a CI regression set; roles, masking and row policies apply; Cortex cost monitored.
  - **Owned by business teams (A14):** each metric and its grains belong to its defining team (finance owns "disbursed amount", lending "EMI collection rate"); changes are reviewed, versioned and certified; no two definitions of one metric.

## 7. Serving it

| Consumer | Reads | Compute | Freshness |
|---|---|---|---|
| Analysts | `gold_<unit>`, `gold_shared`, semantic views | `BI_WH` (Medium, 1–3 clusters) | Intraday ≤ 1 h; yesterday 07:00 IST |
| Finance | `gold_finance` (money movements, reconciliation, breaks, loan-book snapshot), `finance_close` | `FINANCE_WH` (Small) | Yesterday reconciled 09:00; month close working day 2 |
| Data scientists | `gold_features.feat_customer_history`, Silver | Snowpark, or Spark on Iceberg (access: verify) | Yesterday 06:00 |
| Customer apps | `gold_app.app_customer_profile` pushed to DynamoDB every 15 min; API on top (offers, coupons, recommendations, spending insights, cross-business summary) | DynamoDB | ≤ 1 h; worst ≈ 33 min |
| Internal tools | Snowflake read-only role; scheduled extracts | Small warehouse | Per run |
| Auditors | Read-only Silver, `finance_close`, `ops.*`; raw S3 via external table; lineage | — | On request |

**Gold grains** (A14): each Gold table declares grain, owner, refresh and retention in dbt `meta`.
- **Hourly.** Recharge volume and success rate, EMI collections in progress. Business units, Ops; every 15 min; e.g. 90 days.
- **Daily.** Money movements, reconciliation, collections, premiums. Finance, analysts; after the daily gate; 2–5 years (finance 5).
- **Monthly.** Finance close, spending insights, trends. Finance, analysts; monthly rollup; 5 years.
- **Customer-level.** App profile, offer and coupon eligibility, segments, features. Product, data science; every 15 min or daily; current + history as required.
- **Entity-level.** Loan-book snapshot, policy status. Business units; daily; 2–5 years.

**Lending, modelled for lending** (tables: Appendix B). Three facts shape the model:
1. **An unpaid loan emits no events,** so a DPD stored in the last event goes stale exactly when it matters. DPD is computed from the repayment schedule as of a date.
2. **RBI's day-end rule** counts DPD inclusively: an EMI due 31 Mar and unpaid is SMA-1 on 30 Apr, SMA-2 on 30 May and NPA on 29 Jun. NPA is sticky until all arrears are paid.
3. **Aggregates store only sums and counts.** PAR30, collection efficiency, average ticket and weighted rate are ratios computed in the semantic layer, so they roll up correctly.

**Also**
- **App profile.** One row per customer (≈ 2–3 KB × 50M ≈ 100–150 GB): IDs, amounts, statuses only; no PAN, Aadhaar, name or full phone.
- **`finance_close`.** Append-only, written monthly (not a clone; Iceberg clone support: verify).
- **Access.** Roles per consumer group, masking on personal data, row-access policies per unit.
- *Decision sentence:* we chose DynamoDB over serving apps from Snowflake because it gives single-digit ms single-key reads at ≈ $0.75–1.25k/month against a 24×7 warehouse at ≈ $2.2k/month; it costs a second store and an API to keep in sync; it breaks first on write cost if profile churn grows well beyond ~10M updates/day.
- **When it fails.** A failed push leaves apps on the last profile, with an old `data_as_of`. If Snowflake is down, apps are unaffected.

## 8. Deep dive 1 — B: correct latest state

**Problem.** Events arrive twice, late, out of order or by two routes, yet each loan, policy and order must end in exactly the right state.

**Why it is hard**
- "Order by timestamp, take the latest" fails: clocks skew, timestamps tie, transactions run long.
- An auto-sequence fails: a value can be assigned before commit and become visible out of order.
- A plain `MERGE` that always applies the incoming row lets a late, older event overwrite newer state.

**Options considered**
- (1) Order by `occurred_at` or auto-increment ID — rejected: the reasons above.
- (2) Kafka transactions for exactly-once — rejected: they end at the first non-transactional boundary (S3 sink, COPY); the lake write must be idempotent anyway.
- (3) At-least-once + source-assigned sequence + dedup + conditional upsert — chosen.

**Decision**
- The outbox row is written in the same transaction as the business change.
- Its `sequence` is the entity's own version number, incremented in that transaction under a row lock, so sequence order equals commit order. Kafka key = entity ID keeps per-entity order.
- In dbt Silver: dedup on `event_id` (last 8 days, `incremental_predicates`), then the ordering guard: keep an event only if its sequence is higher than the stored one.
- Each payload carries the **full entity state after the change**, so current state is the highest version's payload; field-level `COALESCE` is only a safety net.
- Money comes only through the outbox; fallback CDC uses LSN as sequence and never covers outbox facts.
- Residual risk: a duplicate more than 8 days late is not removed in Silver; daily reconciliation (§9) catches it.

**Guarantee.** For any loan, policy or order, current state equals the event with the highest source sequence among all events received, regardless of how many times or in what order they arrived.

*Decision sentence:* we chose an entity version number plus a conditional upsert over timestamp ordering and Kafka transactions because it is the one ordering the source can prove; it costs a lock-and-increment in each service's transaction and an extra join in every incremental Silver run; it breaks first when a service does not adopt the version rule (then its order is only as good as its CDC log position) or when a duplicate arrives more than 8 days late.

**Code and tests** (lending is the worked example)
- `code/sources/service_db/outbox.sql` — version bump + outbox insert in one transaction.
- `code/connect/debezium-lending-outbox.json` — routes the outbox to Kafka.
- `code/dbt/models/silver/lending/` — `stg_lending_events.sql`; `silver_lending_loan_events.sql` (append-only, dedup); `silver_lending_loans_current.sql` (the guard).
- Unit tests in `_silver_lending.yml` (test 1) — a late older event never overwrites newer state; a newer one replaces it; sequences 9, 8, 9 in one batch apply once at 9.
- `assert_current_matches_latest_history.sql` — current sequence equals the highest in history.
- dbt tests — `unique` and `not_null` on `event_id`; unique `loan_id`. Test plan — replaying a batch twice changes nothing.

## 9. Deep dive 2 — C: exact paise reconciliation

**Problem.** Our record of money (EMIs, premiums, recharges, refunds) must match what partners say happened, and every difference must be explained.

**Why it is hard**
- `SUM(internal) = SUM(partner)` per day lets errors cancel (a missing 500 paise and a duplicate 500 paise net to zero).
- Cut-offs and T+1 settlement put one item on two days; fee and tax lines have no internal twin.
- A partner can say FAILED where we say SUCCESS with identical amounts; an amount tolerance hides real losses.

**Options considered**
- (1) Totals only — rejected: errors cancel.
- (2) Fuzzy amount matching — rejected: a "close" match cannot be explained to an auditor.
- (3) Item-level exact match, then a bounded secondary rule, every leftover classified — chosen.

**Decision**
- **One shape.** `silver.money_movements` (internal) and `silver.partner_records` (vendor files with control totals checked at load, plus API settlement feeds): integer paise, `payment_ref`, event time.
- **Matching.** First exact on `payment_ref` plus vendor; repeats are ranked so pairs are one-to-one and extra copies become DUPLICATE breaks. Second, same customer and amount within ±2 days, mutual-best pairs only. Zero amount tolerance; tolerance only on time.
- **Counterparty.** Lending borrowers repay the lender directly (A15), so the lender's file is the natural counterparty, line for line. Recharge uses the gateway settlement feed in the same shape.
- **Break classes.** Missing internally, missing at partner, amount differs, duplicate, timing (resolves next day), status disagreement. Only transaction lines are matched today; checking fee and tax lines against `ref_fee_rules` is **planned**.
- **Output** per unit, vendor and day. `fct_reconciliation_daily`: RECONCILED or BREAKS_OPEN, difference in paise. `fct_reconciliation_breaks`: one row per unmatched item with reason, owner, age.
- **Partner data never updates Silver state;** if the partner is right, the service emits a correcting event. The partner side uses each record's **current version**; a partner change to a past business date re-reconciles that date and the restatement is logged.
- **Finance** reports only reconciled periods or shows open breaks; month close waits until breaks are resolved or formally accepted.
- **Nightly run.** After the partner gate, re-match the last 7 business days plus any restated day (`delete+insert` by business date); the summary and balance test cover exactly those dates.
- **Health metric.** First-pass auto-match rate per vendor (matched ÷ total, from `fct_reconciliation_daily`); mature payment setups in India run at 85–95%+.
- **Not covered yet (next steps).** Many-to-one settlement matching (one payout = many transactions net of MDR and GST); three-way match against the bank statement on UTR; ledger balance checks (opening + in − out = closing); a manual-match and write-off workflow approved by a second person (maker-checker). Lending needs none for line-level matching; recharge payouts need the first.

**Guarantee.** For every partner and business day, every internal money movement and every partner record is either matched exactly in paise or listed as a classified break with an owner; matched + breaks equals the total on both sides, so no paisa is unaccounted for.

*Decision sentence:* we chose item-level matching with classified breaks and zero amount tolerance over total-vs-total comparison because only itemised matches can be explained to auditors; it costs a daily item-level join of the day's internal money movements (a subset of ~173M events) against ~500M partner lines, finishing ≈ 04:40 with ≈ 4.3 h of slack before 09:00; it breaks first if partners do not carry our `payment_ref` (A6), when the secondary rule must do most of the work.

**Code and tests**
- `code/dbt/models/silver/shared/` — `silver_money_movements.sql`, `silver_partner_records.sql`, `silver_partner_record_changes.sql`, plus the partner snapshot and approval models.
- `code/dbt/models/gold/finance/fct_reconciliation_items.sql` — the matching engine; with `fct_reconciliation_breaks.sql`, `fct_reconciliation_daily.sql`.
- `code/dbt/tests/assert_reconciliation_balances.sql` (test 5) — per vendor and day, both sides: matched + break paise = total paise, same for counts; returns rows only on failure.
- Test plan — one seeded example per break type lands in its class; a cancelling pair (missing 500 paise, duplicate 500 paise) gives two breaks, not zero.

## 10. Living with it

- **Running.** One Airflow DAG every 15 min: copy → dbt Silver per unit → shared Silver → app profile and push → intraday analytics (≈ 6–10 min of the 15). `max_active_runs=1`, `catchup=False`, two retries 2 minutes apart. Daily Gold starts when the gate opens; month close on working day 2.
- **Failure and recovery**
  - Each dbt model is one atomic statement: no table is half-written.
  - The next run re-reads by `_loaded_at` with a 30-minute overlap; writes are idempotent (dedup + guard).
  - Within 90 days: `dbt build --full-refresh` or rebuild from Bronze. Older: a runbook (`COPY … FORCE = TRUE` from S3 into a backfill table, then dbt).
  - Silver and Gold have no Fail-safe (verify): recovery is always "rebuild from S3 raw".
- **Monitoring.** Task and COPY alerts; `dbt source freshness`; a dashboard of lag, counts and `ops.completeness`. Pages: gate closed at 04:30, COPY errors, connector heartbeat missing.
- **Retention**
  - Kafka 7 days; Bronze 90 days; quarantine 1 year.
  - S3 raw 5 years (partner full snapshots 90 days, then month-end snapshots plus daily change sets, from which any day can be rebuilt).
  - Silver and Gold 2–5 years by table and grain (e.g. hourly 90 days, monthly 5 years): `meta: retention_years` in dbt, enforced by a daily delete job by event date; 5-year floor for finance Gold, `finance_close`, `silver.money_movements`, Ops run records.
- **Audit trail.** `ops.run_manifest`: Airflow run ID, dbt run ID, git SHA, input range per run.
- **Cost levers (§3).** BI ≈ 60% of Snowflake: auto-suspend 60 s, result caching, scheduled refresh. Silver storage is the largest storage line: drop the raw JSON payload after 90 days (about half the size).

## 11. Where it breaks first

Each item: what breaks — sign — mitigation.
- **Growth**
  - **MERGE and scan cost on multi-year Silver** — dbt runs creeping past 10 min — `incremental_predicates` (8 days), clustering by load date, retention by unit.
  - **DynamoDB write cost** — writes above ≈ $560/month — write only changed profiles; offers as a separate ≈ 0.5 KB item.
  - **Partner-file window (500M-row load + diff); small sink files (≈ 2 MB)** — partner gate after 04:00, COPY time up — larger warehouse (same credits, faster), files split to ~100–250 MB gzipped, fewer partitions on quiet topics.
- **Operational**
  - **Late partner file** — gate closed at 04:30 (page) — publish provisional, naming the partner; restate later.
  - **Silent stall; duplicate over 8 days late** — freshness alert; reconciliation break — `data_as_of` everywhere; late duplicate caught in §9 and restated.
- **Organisational**
  - **Outbox adoption; vendor format changes** — services still on fallback CDC; header or control-total failure — fallback keeps data flowing; adoption tracked per service; registry contracts; per-vendor mapping config, file held, partner manager alerted.

## 12. Test plan

21 tests, 12 written as code (✅); tests 1 (ordering-guard unit tests) and 5 (balance test) matter most. Full plan, each test naming the mistake it catches: [test-plan.md](test-plan.md).

## Appendix A — Event topics, schema and S3 layout

**Topics** (4 source services, 6 topics `<unit>.<entity>.events`, key = entity ID, 57 partitions sized by assumed share of the 10k/s peak: recharge 60%, lending 30%, insurance 5%, customer 5%)
- `lending.loan_application.events`, 6 partitions: ApplicationSubmitted, Approved, Rejected, OfferAccepted → `bronze.lending_events`.
- `lending.loan.events`, 12: LoanDisbursed, EmiPaid, EmiBounced, LoanForeclosed, LoanWrittenOff → `bronze.lending_events`.
- `insurance.policy.events`, 6: PolicyIssued, PremiumPaid, PolicyLapsed → `bronze.insurance_events`.
- `insurance.claim.events`, 3: ClaimFiled, ClaimApproved, ClaimSettled → `bronze.insurance_events`.
- `recharge.order.events`, 24: RechargeInitiated, RechargeCompleted, RechargeFailed, RefundIssued → `bronze.recharge_events`.
- `customer.profile.events`, 6: CustomerCreated, KycUpdated, ConsentChanged → `bronze.customer_events`.

**Envelope** (Avro in Kafka, backward compatibility by the schema registry; Parquet in S3)
- `event_id` (UUID): dedup key. `event_type`: e.g. `EmiPaid`.
- `aggregate_id`: loan, policy or order ID; also the Kafka key, so one entity stays in one partition, in order.
- `sequence` (integer): the entity's own version number, the ordering key (§8). `occurred_at` (UTC): event time.
- `payload` (JSON): `{customer_id, data{…this change…}, state{…full entity state after it…}}`.
- `topic`, `kafka_partition`, `kafka_offset`, `kafka_ts`: added by the S3 sink (dedup tie-break, offset-continuity check, arrival time). Snowflake adds `_file_name`, `_file_row`, `_loaded_at` at COPY. All money is integer paise.
- Because `state` is the full entity after every change, Silver never merges fields: the highest `sequence` is the current state.

**Payload per topic** (assumed; the brief names no event types) — `data` / `state`
- **loan_application** *(coded).* data: requested amount, product, channel. state: status, approved amount, rate and tenure, rejection reason, submitted / decided / accepted / disbursed times, loan_id.
- **loan** *(coded).* data: amount_paise, payment_ref, installment_no. state: terms, status, outstanding principal, next EMI, dates, **full repayment schedule** (due, paid, fully paid at, bounces).
- **policy.** data: premium_paise, payment_ref, insurer_id. state: status, sum_assured_paise, start/end dates, next_premium_date.
- **claim.** data: claim_amount_paise, policy_id. state: status, approved_amount_paise, settled_at.
- **recharge order.** data: amount_paise, payment_ref, gateway, operator, plan_id. state: status, refund_paise, failure_reason.
- **customer profile.** data: changed fields only. state: kyc_status, consent flags, city (personal data tokenised).

**S3 layout** (one bucket, versioning on; folders by **arrival** time, as topics use `LogAppendTime`; one sink connector per unit, `code/connect/s3-sink-lending.json`; files move to cheaper classes with age)

```
s3://lakehouse-raw/
  bronze/<unit>/<topic>/ingest_date=YYYY-MM-DD/ingest_hour=HH/*.parquet    events, 5-min files, kept 5 years
  landing/<unit>/<vendor>/snapshot_date=D/                                 partner full files + control file
  raw/api/<source>/ingest_date=D/page_NNNNN.json                            API pages, as received
  raw/sheets/<sheet>/snapshot_ts=<ts>/<sheet>.csv                           spreadsheet snapshots
  quarantine/…                                                              rejected files, 1 year
```

## Appendix B — Lending data model

**Silver** (from the two lending topics; every event carries the full loan, schedule included)
- `lending_loan_events` — one row per event; append-only, dedup on `event_id`.
- `lending_loans_current` — one row per loan; ordering guard on `sequence`.
- `lending_installments` — loan × installment (due, paid, bounces); all a loan's rows replaced from its latest state (a restructured schedule can shrink).
- `lending_applications_current` — one row per application; ordering guard on `sequence`.

**Gold** (`gold_lending`, built daily after the internal gate for one date per run; app summary every 15 min) — grain → what it answers
- `fct_lending_loan_daily` — loan × day → DPD, RBI bucket (CURRENT, SMA-0/1/2, NPA), outstanding, overdue, months on book.
- `agg_lending_portfolio_daily` — day × lender × product × bucket → loan book, PAR30/PAR90.
- `agg_lending_disbursals_daily` — day × lender × product × channel → volume, fees, ticket size, weighted rate, cancellations.
- `agg_lending_funnel_daily` — application day × product × channel → submitted → approved → accepted → disbursed.
- `agg_lending_collections_daily` — EMI due day × lender × product → collection efficiency, bounce rate.
- `fct_lending_loan_milestones` — loan → first day at DPD 1/31/61/91, first-payment default.
- `agg_lending_vintage_monthly` — cohort month × MOB → "ever 30+ / 90+ by month n" curves.
- `agg_lending_roll_rates_monthly` — month × from-bucket × to-bucket → loans worsening or curing.
- `app_lending_customer_summary` — customer → active loans, outstanding, next due (no clock; the app shows "N days overdue").

**Definitions that decide correctness**
- DPD = days from the oldest installment not fully paid at day end (IST) + 1; a partial payment leaves it overdue.
- NPA stays NPA until nothing due on or before the date is unpaid, so each day reads the previous day's; a restated day is re-run with every later day, in order.
- Milestones are a pure function of due and full-payment dates, recomputed from the current schedule, so late or reversed payments correct them.
- Vintage uses "ever 30+ by month n": never decreases, compares cohorts fairly.

**Size**
- ~10M active loans → `fct_lending_loan_daily` ≈ 10M rows/day ≈ 0.4 GB/day (≈ 40 B/row). Daily rows kept 13 months (≈ 160 GB), month-end rows 5 years (≈ 600M rows ≈ 25 GB).
- The daily build reads ~120M open installments: minutes on a Medium warehouse.

## Appendix C — Code and tests index

*Written as reviewable code: dbt-snowflake 1.10 parses it cleanly (25 models, 1 snapshot, 31 data tests, 4 unit tests) and every model passes a Snowflake-dialect syntax check; not run against Snowflake. Connector configs and outbox SQL follow documented Debezium / Confluent options but are not run; version-specific keys are flagged in `code/connect/README.md`. Deep dives B and C carry most of the code.*

| Path (under `code/`) | Proves | Dive |
|---|---|---|
| `sources/service_db/outbox.sql` | Business write + outbox row in **one transaction**; `sequence` = loan version under row lock | B |
| `connect/debezium-lending-outbox.json`, `s3-sink-lending.json` | Outbox-only CDC → `lending.<entity>.events`; Kafka → S3 Parquet by arrival hour, 5-min rotation, offsets, DLQ | B, Ops |
| `dbt/models/staging/lending/stg_lending_events.sql`, `silver/lending/silver_lending_loan_events.sql` | Payload parsing, integer paise, UTC; append-only history, dedup on `event_id` (8 days) | B |
| `dbt/models/silver/lending/silver_lending_loans_current.sql` | **The ordering guard** | B |
| `dbt/models/silver/lending/_silver_lending.yml`, `dbt/tests/assert_current_matches_latest_history.sql` | Schema tests + **3 unit tests** for the guard; current = highest sequence | B |
| `dbt/models/silver/lending/silver_lending_installments.sql`, `silver_lending_applications_current.sql` | Schedule replaced from guarded latest state; applications under the same guard | Lending |
| `dbt/models/gold/lending/fct_lending_loan_daily.sql`, `fct_lending_loan_milestones.sql` | **RBI day-end DPD, SMA/NPA bucket**, sticky NPA, IST paid dates; milestones from the schedule | Lending |
| `dbt/models/gold/lending/agg_lending_*`, `gold/app/app_lending_customer_summary.sql` | PAR, disbursals, funnel, collections, vintage, roll rates (additive only); app summary, no clock | Lending |
| `dbt/models/gold/lending/_lending.yml`, `dbt/tests/assert_lending_portfolio_ties_to_loans.sql`, `assert_vintage_curves_never_decrease.sql` | **Unit test of the RBI rule** (bucket edges, UTC/IST trap, sticky NPA); aggregates tie to loans; curves never fall | Lending |
| `dbt/models/staging/partners/stg_partner_lending_nbfc_017.sql`, `partner_snapshot_approval.sql` | Rupee text → exact paise, NULL-safe row hash; truncation guard as data (trailer match, ±10% rows) | C |
| `dbt/snapshots/snap_partner_records.sql`, `dbt/tests/assert_partner_snapshot_not_truncated.sql` | Daily diff of approved full snapshots (SCD2); pages vendor ops when the newest file is held | C |
| `dbt/models/silver/shared/silver_money_movements.sql`, `silver_partner_records.sql`, `silver_partner_record_changes.sql` | Both sides in one shape; partner NEW / CHANGED / REMOVED | C |
| `dbt/models/gold/finance/fct_reconciliation_items.sql`, `fct_reconciliation_breaks.sql`, `fct_reconciliation_daily.sql`, `dbt/tests/assert_reconciliation_balances.sql` | **Matching engine** (exact, mutual-best, classified leftovers; window keyed to run date); breaks with owner and age; **matched + breaks = total** | C |
| `airflow/dags/lakehouse_15min.py`, `lakehouse_partner_daily.py`, `lakehouse_external_daily.py`; `dbt/macros/require_run_date.sql` | 15-min COPY → Silver → app push; partner COPY → guard → snapshot → prune, finance gated on completeness; API and sheet pulls; macro fails without the logical date (no `current_date`) | Ops, B, C |
| `dbt/models/staging/external/stg_api_gateway_settlements.sql`, `silver/external/silver_gateway_settlements.sql`, `staging/external/ref_fee_rules.sql`, `dbt/tests/assert_fee_rules_latest_snapshot_valid.sql` | API pages → partner-record shape; only a fully valid fee-rule snapshot served | C, Ops |
