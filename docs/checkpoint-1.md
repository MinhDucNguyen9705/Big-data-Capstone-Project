# Checkpoint 1

Date: **TBD** (set the milestone date in GitHub too). Rehearsal on day 7: run this file start to finish from a clean
cluster, take screenshots into `docs/img/`, then tag `v0.1-checkpoint`.

## Definition of done (Part A of the implementation plan)

1. **Repo on GitHub**: README quickstart, CI green, `make test` passes.
2. **Executable code**: Binance (and Coinbase) producers running in Kubernetes and writing to Kafka; a Spark
   streaming job writing bronze Parquet to HDFS and 1-minute OHLCV to Cassandra; a batch job loading historical
   archive data into HDFS bronze/silver.
3. **Data ready**: at least 30 days of Binance trades for 10 symbols plus 2 years of 1m klines for 50 symbols in
   HDFS (several GB), plus live data accumulating; `docs/data.md` lists sizes and row counts.
4. **Architecture ready**: `docs/architecture.md` with the Lambda diagram, data flow, deployment diagram, component
   table; ADRs for the main choices; topic, HDFS and Cassandra schemas documented ([contracts.md](contracts.md)).
5. **Dashboard**: a Grafana dashboard showing live 1m candles from Cassandra.

## Checklist per member

### Bootstrap (shared)
- [ ] Fresh clone: `make install test` green, `crypto demo local` prints candles
- [ ] CI green on `main`; branch protection on `main` (require PR + CI)
- [ ] `docs/contracts.md` reviewed by all five members, then frozen
- [ ] Every member ran the tests and the demo on their own machine
- [ ] Issues assigned

### A: Platform
- [ ] A7 kind cluster: `make cluster-up` (1 control plane + 2 workers)
- [ ] A7 Strimzi Kafka + `KafkaTopic` CRs matching `contracts.TOPICS`
- [ ] A7 HDFS NameNode + 2 DataNodes reachable by DNS from Spark pods
- [ ] A7 spark-operator, RBAC, SparkApplication YAMLs for `stream_bronze`, `stream_ohlcv`, `batch_silver`
- [ ] A7 final images (`make build-images`), `make deploy`, `make smoke` with non-zero counts

### B: Ingestion
- [ ] A3 `kafka_io` idempotent producer + DLQ
- [ ] A3 Binance producer (trades + klines), reconnect + gap backfill, metrics
- [ ] A3 Coinbase producer
- [ ] A3 CoinGecko and Fear & Greed pollers (CronJobs)
- [ ] A3 `crypto ingest binance --dry-run` prints validated trades for 30 s

### C: Speed layer
- [x] A5 transforms hardened (streaming dedupe, VWAP edge cases, more rules) with tests
- [x] A6 `stream_bronze` writes bronze Parquet to HDFS (verified on local service processes)
- [x] A6 `stream_ohlcv` with watermark writes `ohlcv_1m` and `latest_price` to Cassandra (local services)
- [x] Integration test shows rows in Cassandra and Parquet files in HDFS (2026-10-21)
- [ ] Deploy both streaming SparkApplications to the team's Kubernetes cluster and validate the running demo

Speed-layer implementation/verification: [speed-layer.md](speed-layer.md),
[streaming journal](journal/streaming.md). The local-service integration gate does not establish that A/E's
Kubernetes deployment, dashboards, History Server or long-running workload are ready.

### D: Batch and data
- [ ] A4 archive downloader with checksum, replayer
- [ ] A6 `batch_load_archive`, `batch_silver` (+ `silver.trades_bucketed`)
- [ ] A8 checkpoint dataset in HDFS; profile job output in `docs/data.md`

### E: Serving and docs
- [ ] A7 Cassandra StatefulSet + schema Job applying `cql/schema.cql`
- [ ] A9 kube-prometheus-stack; `market.json` and `pipeline.json` dashboards with live data
- [ ] A10 architecture doc final, ADRs 0002 to 0005, demo script below
- [ ] A11 3 to 5 real journal entries across categories

## Demo script

| Step | Command | What to show | Expected |
|---|---|---|---|
| 1 | `make cluster-up deploy` | all pods Ready | `kubectl get pods -A` |
| 2 | `make smoke` | topic counts, HDFS usage, Cassandra rows | non-zero counts |
| 3 | Grafana on `http://localhost:3000` | live 1m candles, pipeline dashboard | candles update every minute |
| 4 | `hdfs dfs -ls /lake/silver/trades` | date partitions | 30+ days |
| 5 | `crypto demo local` | the same transforms, no cluster | 60 candles from the fixtures |

(To be completed by E with real outputs and screenshots.)
