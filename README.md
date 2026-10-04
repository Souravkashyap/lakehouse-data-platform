# Lakehouse Data Platform — Design & Hard Parts

A design-and-code exercise: a lakehouse data platform for three consumer businesses (lending, insurance, recharge) that ingests from service databases, event streams, partner files, third-party APIs and Ops spreadsheets, and serves analysts, finance, applications, data scientists and auditors.

**Nothing here is meant to run end to end.** The code covers the two hard problems in depth; surrounding plumbing is stubbed or described.

## Where to start

| Read | What it is |
|---|---|
| [`docs/design.md`](docs/design.md) | The design document: architecture, assumptions, trade-offs, cost, and where it breaks first |
| [`docs/test-plan.md`](docs/test-plan.md) | The test plan: what each test asserts, and how it would fail if the design were wrong |
| `code/dbt/` | dbt project: Silver (ordering guard), partner snapshot diff, reconciliation engine, and the tests (3 unit tests + singular data tests) |
| `code/airflow/dags/` | Three skeleton DAGs: the 15-minute pipeline, the daily partner/finance pipeline, and the daily API + spreadsheet pull |
| `code/sources/`, `code/connect/` | Service-side outbox write (Postgres) and the Debezium + S3 sink connector configs |

## Stack, and what it buys

| Layer | Choice |
|---|---|
| Change capture | Transactional outbox + Debezium (log-based CDC) |
| Transport | Kafka (Amazon MSK) |
| Raw storage | S3 (Parquet), 5 years |
| Engine and storage format | Snowflake; Silver and Gold as Snowflake-managed **Iceberg** tables in our own S3 |
| Transformation | dbt |
| Orchestration | Airflow (Amazon MWAA) |
| App serving | DynamoDB |

Snowflake, Kafka, Airflow, Postgres and AWS are tools I've run in production, so the effort can go into the hard parts rather than the plumbing. *(Author: adjust this line to match your own experience exactly.)*

## The two hard problems (in code)

- **B: correct latest state** under duplicate, late and out-of-order delivery. Outbox in the same transaction, the entity's version number as the ordering key, and a dbt ordering guard.
- **C: every paisa matches.** Exact reconciliation of internal money movements against partner files, with classified breaks and a test proving that matched + breaks = total on both sides.

## Layout

```
docs/              design document, test plan, architecture diagram
code/sources/      service_db/outbox.sql — business write + outbox row in one transaction
code/connect/      Debezium outbox connector, Kafka → S3 sink connector
code/dbt/          models (staging, silver, gold), snapshots, tests, macros
code/airflow/dags/ lakehouse_15min.py, lakehouse_partner_daily.py, lakehouse_external_daily.py
```
