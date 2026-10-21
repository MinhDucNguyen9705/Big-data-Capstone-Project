# Streaming

Scope: Structured Streaming: watermarks, state, output modes, exactly-once. Main contributors: C. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-21: Split stateful queries and verify recovery

* **Context / symptom:** A5 requires 2-minute streaming dedupe and A6 requires 30-second watermarked candles
  in update mode. Spark 3.5 disallows chaining multiple stateful operators in update mode. Single-file text
  streams also failed at execution with `Option 'basePath' must be a directory`.
* **What we tried:** used a durable deduped Parquet handoff with separate query checkpoints; for single-file
  input, read the parent directory with a pathGlobFilter. Preserved Kafka lineage and invalid raw payloads.
* **Numbers:** local fixture run processed 500 raw rows: 3 invalid, 5 duplicates removed, 492 unique trades,
  60 candle updates. With AvailableNow and a 30-second candle watermark, 54 windows finalized; the last 6
  stayed open. These are fixture results, not production throughput/latency measurements.
* **Fix / decision:** `stream_ohlcv` has separate dedupe, quarantine, live-candle, latest-price and gold
  queries. Tests exercise multiple input files, duplicate delivery across restarts, late-drop state metrics,
  cumulative candle updates, final-window emission and concurrent-query cleanup.
* **Lesson:** update mode needs an independently checkpointed boundary between stateful stages. A watermark
  advances with event time; stopping a finite source does not force every window closed.

## 2026-10-21: Real Kafka/HDFS/Cassandra connector test

* **Context / symptom:** distributed latest-price writes failed on Cassandra with
  `SyntaxError: no viable alternative at character '_'` when the per-row writetime alias was `_write_ts`.
  The connector uses it as an unquoted CQL bind marker, whose name must begin with a letter.
* **What we tried:** ran actual Kafka 3.9.1 (KRaft), HDFS 3.3.6 (one NameNode + one DataNode) and Cassandra
  5.0.6 locally, with Spark 3.5.9 / Python 3.11 / Java 17 and the connector assembly 3.5.1. Changed the alias
  to `write_ts`; retained event_ts * 1000 and the datasource writetime option.
* **Numbers:** `tests/integration/test_speed_layer.py -m integration -vv -s` passed in 33.76 s. The test
  published 4 trades (3 unique, 1 duplicate), 1 malformed record and 1 kline. Bronze remained at 5 trade
  rows and 1 kline row after recovery; quarantine contained 1 row. The sampled candle had open 100,
  high 110, low 90, close 110, volume 4, quote_volume 410, VWAP 102.5 and 3 trades. Rewriting its output
  batch preserved its values. A later watermark-advancing trade finalized exactly one gold candle with
  3 trades. Writing the older 100-price trade after the stream left latest_price at 110 with qty 2.
* **Fix / decision:** the integration gate compares Cassandra to the same raw data stored on real HDFS,
  retries the connector sink, and restarts jobs from HDFS checkpoints. Application manifests also passed
  validation against the Spark Operator v2.3.0 SparkApplication CRD.
* **Lesson:** exercising real connectors catches CQL and option behavior that a mocked writer cannot.
  This test used local service processes, not Kubernetes; no cluster soak, driver-pod kill drill, deployment
  image build or History Server validation is claimed.

Final repository check on 2026-10-21: `make lint test` (Python 3.11 / Java 17) passed lint/format and
**93 tests**, with 13 existing ingestion/batch stubs skipped and the 1 service integration test deselected.
The service integration test above was run separately and passed. The local continuous-query test also
started all five OHLCV queries concurrently, delivered another file, and verified cleanup on interruption.
