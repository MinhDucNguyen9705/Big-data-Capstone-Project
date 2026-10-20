# ADR 0001: Lambda architecture (not Kappa)

* Status: accepted (2026-10-20)
* Deciders: whole team (bootstrap)
* Related: [architecture.md](../architecture.md), [contracts.md](../contracts.md)

## Context

We need live 1-minute candles with seconds of latency, and analytics over years of history (indicators,
correlations, ML). History comes from two places: the live Kafka stream and the Binance public archive, which
is zipped CSV that never passes through Kafka. Kafka retention is 7 days on the local cluster. The course
evaluates both batch skills (Spark SQL, joins, bucketing, MLlib) and streaming skills (watermarks, output modes,
state).

## Options considered

1. **Kappa**: one streaming pipeline; reprocess history by replaying Kafka. Simple to run, but needs infinite
   retention or tiered storage, and the CSV archive would have to be pushed through Kafka by a replayer first.
2. **Lambda**: a speed layer for low latency plus a batch layer over an immutable master dataset, merged in the
   serving layer.

## Decision

Lambda. The speed layer is Structured Streaming `raw.trades` -> 1m OHLCV -> Cassandra. The batch layer is
Kafka -> HDFS bronze (`stream_bronze`) plus archive loads (`batch_load_archive`) -> silver -> gold with Spark batch
jobs. Both layers call the same pure transform functions in `cryptopipe.transforms`, which removes the classic
Lambda cost of maintaining two codebases.

## Consequences

* + The batch layer is the source of truth: it includes trades that arrive after the streaming watermark and the
  archive history. Any bug in the speed layer is fixed by recomputing from bronze.
* + Batch-only features (bucketing, CBO, MLlib training) have a natural home.
* - Two pipelines to run and monitor; the serving query must merge batch views with live data (B2.7).
* - Late data handling differs by layer (dropped after the watermark in the speed layer, included in batch); this
  difference is documented and measured in B3.
* The replayer (`crypto replay`) is kept for load tests and busy demos, not for reprocessing.
