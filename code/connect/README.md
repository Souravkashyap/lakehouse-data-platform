# Kafka Connect configs (design section 5 and 8)

- `debezium-lending-outbox.json`: reads the lending service's `outbox` inserts from the Postgres WAL (pgoutput) and routes each to `lending.<aggregatetype>.events`.
  The message key is `aggregateid`, so one loan always lands in one partition and stays ordered.
- `publication.autocreate.mode=filtered`: the publication covers only `public.outbox` (the default, all tables, needs superuser).
- `snapshot.mode=no_data`: the outbox is emptied after each insert, so there is nothing to snapshot; the slot starts at the WAL position.
- `s3-sink-lending.json`: writes the topics to S3 as Parquet, one folder per hour of ingest. `InsertField` adds
  `topic, kafka_partition, kafka_offset, kafka_ts`, the Bronze columns used for dedup and the offset-continuity check.
- Topic config must set `message.timestamp.type=LogAppendTime`, so folders follow broker ARRIVAL time (not event time).
- Rotation by wall clock (`rotate.schedule.interval.ms`) is NOT exactly-once: after a task restart the same
  records can land in a second file. That is expected; Silver dedups on `event_id` (design section 8).
- Bad records go to `dlq.s3sink.lending` with headers; `errors.tolerance=all` means the DLQ depth must be alerted on.
- Some keys are version-specific (EventRouter field names, `snapshot.mode` values, S3 sink options):
  verify against the deployed Debezium and Confluent versions before use.
- Apply with `curl -X PUT -H 'Content-Type: application/json' --data @file.json host:8083/connectors/<name>/config` (send only the `config` object).
