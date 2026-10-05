# Lakehouse Data Platform for Lending, Insurance and Recharge — Design Doc

**Author:** Sourav Kashyap · **Status:** Draft for review · **Updated:** 5 Oct 2026 · **Code:** `code/` · **Tests:** `docs/test-plan.md`

## 0. Summary

One data platform for lending, insurance and recharge. Each business writes its changes as events in the same transaction as the change, so nothing is lost or invented. Events flow through Kafka into S3; Snowflake turns them into tables for analysts, finance, data scientists, auditors and apps. Raw data is kept 5 years, so any number can be rebuilt.

- **Correct latest state (§8, code).** Each loan, policy and order ends in its right state, however events are duplicated, late or out of order.
- **Every paisa matches (§9, code).** Our books are proven against partner records, or every difference is listed and owned.
- **Completeness rule (§6, design only).** A table is published as final only when the day's data is shown to be complete.

## 1. Context & assumptions

**From the brief:** five source types (service databases, event streams, partner files, third-party APIs, Ops spreadsheets); ~10k events/s at peak and ~500M file rows/day; 50M customers; 5 years of history; money in integer paise. No schemas, event types, file formats or latency targets are given, so the items below are our assumptions.

**Key assumptions**

| # | Assumption | If wrong |
|---|---|---|
| A1 | Peak 10k events/s on the busiest days; average ~2,000/s ≈ 173M events/day. | At 10k/s average (864M/day), cost ≈ 5×. |
| A5 | Partners send a full daily snapshot (~500M rows, ~30–50 vendors, by 02:00 IST); ~5% changes daily. | New-rows-only files make the diff step (§5) unnecessary. |
| A6 | Partner lines carry our `payment_ref`. | The weaker secondary match does most of the work (§9). |
| A8 | SLAs: apps ≤ 1 h; analysts by 07:00 IST; data scientists 06:00; finance reconciled 09:00. | Seconds-level freshness needs streaming. |
| A15 | Lending: we are a Lending Service Provider; loans sit on lenders' books (~10M active, EMIs), so the lender's classification is official. | Revolving credit (BNPL) needs a different model. |

**Other assumptions**
- A7. One customer ID across businesses; no identity resolution.
- A11. AWS Mumbai (`ap-south-1`); payment data stays in India (RBI).
- A13. Services publish business events via the outbox with one envelope (`event_id`, entity ID, `sequence`, `occurred_at`).
- A14. Business teams own Gold grains, retention and metric definitions through a short data contract.

## 2. Requirements, SLAs and non-goals

- **Must do**
  - Land all sources in a lake; build Silver and Gold in an open table format in our own S3.
  - Keep 5 years: raw, finance, money and audit tables 5 years; other Silver and Gold per business-team requirement, 2–5 years.
  - Show completeness and freshness per table; meet A8; reproduce any reported number.
- **Must not do:** sit on a transaction's critical path; change a closed month silently; match money on "close" amounts.
- **Non-goals:** real-time fraud or online ML; identity resolution (A7); multi-region DR (A11); clickstream; a full privacy design (tokenisation plus crypto-shredding is the likely route).

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
- **Compute is about half the bill, and BI is 60% of it:** auto-suspend (60 s), result caching and scheduled refresh are the levers.
- **Storage is the line that grows:** keeping partner changes instead of full daily snapshots saves ≈ 48 TB over 5 years; dropping the raw Silver payload after 90 days (about half its size) is the next lever.

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
  - Nightly COPY of the full snapshot (Large) → per-vendor Bronze (last 2 snapshots) → mapping config + row hash.
  - **Truncation guard:** row count within ±10% of yesterday, control totals present. **A failing snapshot is held and no records are marked removed**; a cut-off file would look like millions of deletions.
  - **Daily diff vs yesterday (dbt snapshot):** new key = NEW, changed hash = CHANGED, missing key = REMOVED; unchanged rows (~95%) do nothing. Output: `silver.partner_records` (current versions) + `silver.partner_record_changes`.
  - A changed record for a past business date is a partner restatement: that date is re-reconciled (§9).
  - `loaded + rejected ≠ trailer rows` or paise total ≠ trailer total: file held. Over 1% rows rejected: quarantined, partner manager alerted. Missing file: page at 04:30 (§6).
- **Third-party APIs** (example: payment gateway settlements, for recharge).
  - Daily Airflow pull at 01:30 for day D; cursor pagination; each raw page stored untouched under a fixed key (`ingest_date=D/page_NNNNN.json`).
  - COPY into VARIANT Bronze → one row per settlement (exact paise, newest copy wins) → `silver_gateway_settlements` in the same shape as partner records, so it joins reconciliation unchanged. Incremental feed: no snapshot diff.
  - 429: wait for `Retry-After`; 5xx: exponential backoff, then task retry. A rerun overwrites the same keys, so it is idempotent. A missing day shows as missing-at-partner breaks (§9).
- **Ops spreadsheets** (example: fee rules).
  - Each daily run exports the whole sheet to S3 as a CSV snapshot (every pull kept) → text-only Bronze → `ref_fee_rules`.
  - Every row is checked (unique product code, fee 0–10,000 bps, rupees → exact paise, valid date range); a snapshot is used only if all rows pass.
  - **A bad edit never replaces good rules:** the last valid snapshot keeps serving and a test pages the sheet owner.

Topics, event envelope, payloads and S3 layout (A13): Appendix A. Code per source: Appendix C.

**Schema changes**
- Avro schemas with backward compatibility are enforced in the registry: a breaking change is rejected before Kafka.
- New optional fields flow through in the raw payload and are mapped into Silver when a team needs them.
- Vendor format changes are caught by the per-vendor header check and fixed in that vendor's mapping config.

**Completeness of loading**
- Every COPY result is checked; errored files go to quarantine.
- A file ledger compares the S3 listing with Snowflake load history; a file unloaded after 30 min alerts.
- A per-partition Kafka offset-continuity check proves nothing was lost to Snowflake (whether producer transactions leave offset gaps: verify).

## 6. Making it trustworthy

| Property | How we get it | Where |
|---|---|---|
| Correct | Ordering guard (state); item-level reconciliation (money) | §8, §9 |
| Complete | Two-tier completeness gate (below) | `ops.completeness` |
| Fresh | SLAs per A8; `data_as_of` on every table; `dbt source freshness` alerts on Bronze | §10 |
| Traceable | Run manifest and lineage; raw S3 rebuilds any number | §10 |
| Consistent meaning | Each metric defined once, owned by its business team (A14) | below |

**Completeness gate** (hard problem A, design only)

| Tier | For | Rule | Timing |
|---|---|---|---|
| Intraday | Apps, intraday analysts | Non-blocking: publish what has arrived, stamped with `data_as_of` | Worst ≈ 33 min: sink 5 + COPY wait 15 + load 2 + Silver 3 + app Gold 3 + push 5 |
| Daily: internal gate | Data scientists 06:00, analysts 07:00 | Kafka offsets reach Silver with no gaps, counts match at every hop (Kafka, S3, Bronze, Silver), Debezium heartbeats live. Day D = arrival hours through D+1 01:00 IST (1 h allowed lateness, tuned to p99.9 of arrival minus event time) | Opens ≈ 01:30; done ≈ 02:30 |
| Daily: partner gate | Finance 09:00 | Internal gate, plus every expected partner file for D loaded, matching its control totals, and diffed | ≈ 03:00–03:40; reconciliation done ≈ 04:40 |

- **Guarantee.** A business day reaches finance Gold only when every unit's events and every expected partner file are loaded with no offset gaps and matching counts at every hop.
- **When a source is late.** Page at 04:30 if the gate is still closed; at 06:00 and 07:00 the day is published as provisional, naming the missing source. **Finance never gets an incomplete day as final.**
- **When late data changes a published day.** Open month: day D is restated in the next run and logged in `ops.restatements`. Closed month: `finance_close` stays unchanged; the change is a prior-period adjustment in the current month.
- *Decision sentence:* we chose a completeness gate with allowed lateness over a fixed-time cutoff and freshness-only checks, because a fixed cutoff publishes incomplete days silently and "recent" does not mean "whole"; it costs a late publish when a source is late (finance sees "partner X pending", not a wrong figure); it breaks first when one chronically late partner blocks the gate every night. Source-emitted "hour closed" markers are a stronger upgrade but need every service changed.

**Consistent meaning (semantic layer)**
- Metrics are defined once in dbt, owned by one team each (finance: "disbursed amount"; lending: "EMI collection rate"), and served as Snowflake semantic views to BI and Cortex Analyst.
- Guardrails for plain-English answers: approved metrics on certified Gold only; every answer shows its SQL; ~30–50 known questions run in CI.
- **Official finance figures come only from `gold_finance` and `finance_close`.**

## 7. Serving it

| Consumer | Reads | Compute | Freshness |
|---|---|---|---|
| Analysts | `gold_<unit>`, `gold_shared`, semantic views | `BI_WH` (Medium, 1–3 clusters) | Intraday ≤ 1 h; yesterday 07:00 IST |
| Finance | `gold_finance` (money movements, reconciliation, breaks, loan-book snapshot), `finance_close` | `FINANCE_WH` (Small) | Yesterday reconciled 09:00; month close working day 2 |
| Data scientists | `gold_features.feat_customer_history`, Silver | Snowpark, or Spark on Iceberg (access: verify) | Yesterday 06:00 |
| Customer apps | `gold_app.app_customer_profile` pushed to DynamoDB every 15 min; API on top (offers, coupons, recommendations, spending insights, cross-business summary) | DynamoDB | ≤ 1 h; worst ≈ 33 min |
| Internal tools | Snowflake read-only role; scheduled extracts | Small warehouse | Per run |
| Auditors | Read-only Silver, `finance_close`, `ops.*`; raw S3 via external table; lineage | — | On request |

**Gold grains** (A14): each Gold table declares its grain (hourly, daily, monthly, customer- or entity-level), owner, refresh and retention (2–5 years; finance 5) in dbt `meta`, set by the owning business team.

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

**Why these two problems.** I picked the failures that are silent and costly:
- **They corrupt money or what a customer sees,** not just a dashboard.
- **They don't fail loudly:** pipelines stay green while state or totals drift (a late event wins; a missing ₹500 cancels a duplicate ₹500).
- **They need a guarantee, not a setting:** no tool option fixes them.
- **Not chosen:** completeness is designed (§6) but not coded; schema drift is handled by registry contracts (§5); customer identity is out of scope (A7).

## 8. Deep dive 1 — B: correct latest state

**Problem.** Events arrive twice, late, out of order or by two routes; each loan, policy and order must still end in exactly the right state.

| Option | Verdict | Why |
|---|---|---|
| Order by `occurred_at` | Rejected | Clocks skew, timestamps tie, transactions run long |
| Auto-increment ID | Rejected | Assigned before commit, so it can become visible out of order |
| Plain `MERGE` (always apply the incoming row) | Rejected | A late, older event overwrites newer state |
| Kafka transactions (exactly-once) | Rejected | Stop at the S3 sink and COPY; the lake write must be idempotent anyway |
| **At-least-once + source sequence + dedup + conditional upsert** | **Chosen** | The one ordering the source can prove |

**How it works**
- The outbox row is written in the same transaction as the business change.
- `sequence` = the entity's own version, bumped under a row lock, so sequence order = commit order; Kafka key = entity ID.
- Silver drops duplicates on `event_id` (last 8 days), then the **ordering guard** applies an event only if its sequence is higher.
- Every event carries the **full entity state**, so the highest version *is* the current state.
- Money comes only through the outbox; fallback CDC uses the log position (LSN) as sequence.

**Decision summary**

| | |
|---|---|
| **Guarantee** | Current state = the event with the highest source sequence received, however many times and in whatever order events arrive |
| **Cost** | A lock-and-increment in each service transaction; an extra join in every incremental Silver run |
| **Breaks first** | A service skips the version rule (its order is then only as good as its CDC log position); a duplicate arrives more than 8 days late (caught by reconciliation, §9) |

**Code and tests** (lending is the worked example)

| File | Proves |
|---|---|
| `sources/service_db/outbox.sql`, `connect/debezium-lending-outbox.json` | Change and event commit together, then route to Kafka |
| `silver_lending_loan_events.sql`, `silver_lending_loans_current.sql` | Append-only history with dedup; the ordering guard |
| `_silver_lending.yml` (test 1), `assert_current_matches_latest_history.sql` | A late older event never wins; 9, 8, 9 in one batch applies once at 9; current = highest sequence in history |

## 9. Deep dive 2 — C: exact paise reconciliation

**Problem.** Our record of money (EMIs, premiums, recharges, refunds) must match what partners say happened, and every difference must be explained. Hard because cut-offs and T+1 settlement put one item on two days, fee and tax lines have no internal twin, and a partner can say FAILED where we say SUCCESS for the same amount.

| Option | Verdict | Why |
|---|---|---|
| Compare daily totals | Rejected | Errors cancel: a missing 500 paise and a duplicate 500 paise net to zero |
| Fuzzy amount matching | Rejected | A "close" match cannot be explained to an auditor; tolerance hides real losses |
| **Item-level exact match, bounded secondary rule, every leftover classified** | **Chosen** | Every paisa is either matched or an owned break |

**Matching rules:** zero tolerance on amount; tolerance only on time.

| Step | Rule | Result |
|---|---|---|
| 1. Exact | Same vendor + `payment_ref`; repeats ranked so pairs are one-to-one | MATCHED_EXACT, or AMOUNT_DIFFERS / STATUS_DISAGREES; extra copies are DUPLICATE |
| 2. Secondary | Same customer + same amount, within ±2 days; mutual-best pairs only | MATCHED_SECONDARY |
| 3. Leftovers | Only on our side (recent / older) or only on the partner's side | TIMING / MISSING_AT_PARTNER / MISSING_INTERNALLY |

**How it runs**
- **Inputs:** `silver.money_movements` vs `silver.partner_records` (vendor files and API settlement feeds), both in integer paise with `payment_ref`.
- **Counterparty:** the lender's file for lending, since borrowers repay the lender directly (A15); the gateway feed for recharge.
- **Nightly:** after the partner gate, re-match the last 7 business days plus any restated day; the summary and balance test cover exactly those dates.
- **Outputs:** daily status per vendor (RECONCILED / BREAKS_OPEN) and one break row per item with reason, owner and age.
- **Rules:** partner data never changes Silver (the service emits a correcting event); finance reports only reconciled days; month close waits for breaks.
- **Health metric:** first-pass auto-match rate per vendor; mature setups in India run at 85–95%+.
- **Not covered yet:** fee lines vs `ref_fee_rules` (planned) · many-to-one settlement net of MDR and GST (needed for recharge payouts) · bank statement on UTR · ledger balances · maker-checker write-offs.

**Decision summary**

| | |
|---|---|
| **Guarantee** | Every internal money movement and partner record is matched exactly in paise or listed as a classified break with an owner; matched + breaks = total on both sides, per partner and day |
| **Cost** | A daily item-level join of the day's money movements (a subset of ~173M events) against ~500M partner lines; done ≈ 04:40, ≈ 4.3 h before the 09:00 SLA |
| **Breaks first** | Partners stop carrying our `payment_ref` (A6), so the weaker secondary rule does most of the work |

**Code and tests**

| File | Proves |
|---|---|
| `silver_money_movements.sql`, `silver_partner_records.sql`, `silver_partner_record_changes.sql` + partner snapshot models | Both sides in one shape; partner corrections tracked |
| `fct_reconciliation_items.sql`, `fct_reconciliation_breaks.sql`, `fct_reconciliation_daily.sql` | The matching engine; breaks with owner and age; status per day |
| `assert_reconciliation_balances.sql` (test 5) | matched + breaks = total, in paise and counts, both sides; seeded break examples are in the test plan |

## 10. Living with it

- **Running.** One Airflow DAG every 15 min: copy → dbt Silver → app profile and push (≈ 6–10 min of the 15); one run at a time, two retries. Daily Gold starts when the gate opens.
- **Failure and recovery**
  - Each dbt model is one atomic statement: no table is half-written.
  - The next run re-reads by `_loaded_at` with a 30-minute overlap; writes are idempotent (dedup + guard).
  - Within 90 days: `dbt build --full-refresh` or rebuild from Bronze. Older: a runbook (`COPY … FORCE = TRUE` from S3 into a backfill table, then dbt).
  - Silver and Gold have no Fail-safe (verify): recovery is always "rebuild from S3 raw".
- **Monitoring.** Task and COPY alerts; `dbt source freshness`; a dashboard of lag, counts and `ops.completeness`. Pages: gate closed at 04:30, COPY errors, connector heartbeat missing.
- **Retention**
  - Kafka 7 days; Bronze 90 days; quarantine 1 year.
  - S3 raw 5 years (partner full snapshots 90 days, then month-end snapshots plus daily change sets, from which any day can be rebuilt).
  - Silver and Gold 2–5 years per table (`meta: retention_years`, daily delete job); 5-year floor for finance tables, `silver.money_movements` and run records.
- **Audit trail.** `ops.run_manifest`: Airflow run ID, dbt run ID, git SHA, input range per run.

## 11. Where it breaks first

Each item: what breaks — sign — mitigation.
- *Growth*
  - **MERGE and scan cost on multi-year Silver** — dbt runs creeping past 10 min — `incremental_predicates` (8 days), clustering by load date, retention by unit.
  - **DynamoDB write cost** — writes above ≈ $560/month — write only changed profiles; offers as a separate ≈ 0.5 KB item.
  - **Partner-file window (500M-row load + diff); small sink files (≈ 2 MB)** — partner gate after 04:00, COPY time up — larger warehouse (same credits, faster), files split to ~100–250 MB gzipped, fewer partitions on quiet topics.
- *Operational*
  - **Late partner file** — gate closed at 04:30 (page) — publish provisional, naming the partner; restate later.
  - **Silent stall; duplicate over 8 days late** — freshness alert; reconciliation break — `data_as_of` everywhere; late duplicate caught in §9 and restated.
- *Organisational*
  - **Outbox adoption; vendor format changes** — services still on fallback CDC; header or control-total failure — fallback keeps data flowing; adoption tracked per service; registry contracts; per-vendor mapping config, file held, partner manager alerted.

## 12. Test plan

21 tests, 12 written as code (✅); **tests 1 (ordering-guard unit tests) and 5 (balance test) matter most.** Full plan, each test naming the mistake it catches: [test-plan.md](test-plan.md).

## 13. Honesty: limitations and AI use

- **Assumed, not given:** every volume, schema, event type and SLA (§1). Costs use list prices, not Mumbai quotes.
- **Not built:** the completeness gate (design only); insurance and recharge Silver (lending is the worked example); many-to-one settlement and the bank-statement leg (§9); multi-region DR; a full privacy design.
- **Unverified:** nothing has run on Snowflake. The code parses and passes a Snowflake-dialect syntax check, but the unit tests have not executed. Snowflake-managed Iceberg details (clustering, VARIANT, Fail-safe), the connector settings and the partner-load time need checking.
- **Simplified:** DPD treats a partial payment made after day D as unpaid on D; principal outstanding is on a scheduled basis.
- **AI use:** I used Claude (Anthropic) to research options, draft this document, the code and the tests, and review them. The design decisions are mine, including Snowflake over Databricks, dbt in Airflow over Streams and Tasks, Iceberg for Silver and Gold, DynamoDB for apps, full partner snapshots, and lending modelled on RBI rules.

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
- **loan** *(coded).* data: amount_paise, payment_ref, installment_no. state: terms, status, outstanding principal, next EMI, dates, full repayment schedule (due, paid, fully paid at, bounces).
- **policy.** data: premium_paise, payment_ref, insurer_id. state: status, sum_assured_paise, start/end dates, next_premium_date.
- **claim.** data: claim_amount_paise, policy_id. state: status, approved_amount_paise, settled_at.
- **recharge order.** data: amount_paise, payment_ref, gateway, operator, plan_id. state: status, refund_paise, failure_reason.
- **customer profile.** data: changed fields only. state: kyc_status, consent flags, city (personal data tokenised).

**S3 layout** (one bucket, versioning on; folders by arrival time, as topics use `LogAppendTime`; one sink connector per unit, `code/connect/s3-sink-lending.json`; files move to cheaper classes with age)

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

*25 models, 1 snapshot, 31 data tests, 4 unit tests; parses and passes a Snowflake-dialect syntax check, not run (§13). Connector keys that vary by version are flagged in `code/connect/README.md`.*

| Path (under `code/`) | Proves | Dive |
|---|---|---|
| `sources/service_db/outbox.sql` | Business write + outbox row in one transaction; `sequence` = loan version under row lock | B |
| `connect/debezium-lending-outbox.json`, `s3-sink-lending.json` | Outbox-only CDC → `lending.<entity>.events`; Kafka → S3 Parquet by arrival hour, 5-min rotation, offsets, DLQ | B, Ops |
| `dbt/models/staging/lending/stg_lending_events.sql`, `silver/lending/silver_lending_loan_events.sql` | Payload parsing, integer paise, UTC; append-only history, dedup on `event_id` (8 days) | B |
| `dbt/models/silver/lending/silver_lending_loans_current.sql` | **The ordering guard** | B |
| `dbt/models/silver/lending/_silver_lending.yml`, `dbt/tests/assert_current_matches_latest_history.sql` | Schema tests + 3 unit tests for the guard; current = highest sequence | B |
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
