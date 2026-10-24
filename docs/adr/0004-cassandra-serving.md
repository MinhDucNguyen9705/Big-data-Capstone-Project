# ADR 0004: Cassandra for serving market views

Status: implemented in checkpoint configuration; cluster acceptance remains a separate check.

## Context

The speed layer produces cumulative minute candles and latest prices. Serving must support frequent writes
and bounded reads by symbol/time without querying raw Kafka or scanning historical Parquet for every refresh.

## Decision

Use the existing Cassandra contract: `crypto.ohlcv_1m` partitioned by `(symbol, day)` with clustering
`(window_start, exchange)`, and `crypto.latest_price` keyed by `(symbol, exchange)`. Use the existing Spark
connector writers. Deploy one persistent Cassandra 5.0.6 node for the local checkpoint; apply the canonical
`cql/schema.cql` through a re-runnable Kubernetes Job.

Grafana uses the signed HadesArchitect Cassandra datasource 3.3.0. The market view fixes a symbol and reads
at most two UTC day partitions for ranges no longer than 24 hours. Exchange filtering after the timestamp
range uses bounded `ALLOW FILTERING` due to the frozen clustering order; no new index/table is introduced.

## Consequences

- Candle upserts reuse primary keys. This preserves the existing at-least-once writer semantics, not a
  cross-sink transaction or a stronger exactly-once guarantee.
- Cassandra keeps 30-day hot candles; HDFS retains history. Grafana/Prometheus retention is independent.
- The one-node RF=1 deployment preserves data across pod recreation through its PVC but offers no
  tolerance of permanent disk/node loss. Authentication, TLS and HA belong to the later cloud/security work.
- A general historical query UI needs a serving/query design beyond this bounded live dashboard.
- MongoDB or Elasticsearch would add a separate data model/indexing path. They are alternatives, not current
  components; Cassandra fits the project's already agreed write/query contracts.
