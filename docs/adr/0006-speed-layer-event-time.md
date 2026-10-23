# ADR 0006: Speed-layer event time, watermark separation and Cassandra upserts

Date: 2026-10-21. Status: implemented in the speed_layer branch; deployment validation is tracked separately.

## Context

A5 specifies two-minute watermarked dedupe, while A6 specifies 30-second watermarked one-minute candles in
update mode. A direct dedupe → aggregation chain violates Spark 3.5's restriction on multiple stateful operators
in update mode. Kafka redelivery, reconnect overlap, event reordering and sink retries must not inflate candles.

## Decision

Use the shared Trade schema and business key, exchange event_ts (UTC ms), two-minute dedupe and a durable
Parquet handoff under the checkpoint root. Independent consumers compute update-mode live candles, append-mode
final candles and latest prices. Each query owns a distinct checkpoint; each file output has one streaming owner.

Use existing Cassandra primary keys. Write cumulative candles by upsert, and write latest-price cells with
per-row event_ts * 1000 as Cassandra writetime. All connector writes execute on Spark executors. Keep endpoints
in Settings and tuning in SparkApplication manifests/job arguments. Rejected records go to quarantine with raw
payload/reason; bronze retains the source data for later batch correction.

## Consequences

State is bounded by the respective watermarks, and each query can recover independently. The handoff costs
storage and another trigger; retain it along with checkpoints. It is not automatically compacted or pruned.
There is no atomic transaction across sinks. Replay preserves candle values by key but can refresh updated_at
and TTL. Equal-event-time latest-price conflicts follow Cassandra cell tie resolution. Finalization depends on
incoming event time; the last window stays open when input is idle. Changes to state structure or watermarks
require an explicit migration/rebuild plan, not reusing incompatible checkpoints.

## Evidence

Local file-stream tests cover cross-batch dedupe, late-drop metrics, cumulative updates, final-window emission
and checkpoint recovery. The service integration gate is `tests/integration/test_speed_layer.py`. Cluster soak
and driver failure evidence belongs in the journal; tests with local Parquet do not prove deployed behavior.

References: [Spark unsupported operations](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#unsupported-operations),
[Spark foreachBatch semantics](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#foreachbatch),
[Cassandra connector per-row writetime](https://github.com/apache/cassandra-spark-connector/blob/v3.5.1/doc/reference.md#cassandra-datasource-table-options).
