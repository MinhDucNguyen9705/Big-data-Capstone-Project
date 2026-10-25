<!-- Owner: E (Serving and docs). -->
# Architecture

This describes the intended integrated deployment. Code/manifests existing in Git do not establish cluster
readiness. How to run it: [getting-started.md](getting-started.md). Platform details: [deployment.md](deployment.md).

## 1. Problem and 3V numbers

Crypto markets trade 24/7 on many exchanges. The system collects every trade and 1-minute candle from Binance
and Coinbase in real time, keeps the full history in a data lake, and serves both live candles (seconds of
latency) and historical analytics (indicators, correlations, ML; milestones B2 to B4).

| V | Measured / planned | Source |
|---|---|---|
| **Velocity** | To measure on the integrated workload: producer rate, Spark query input/processing rates and trigger duration. Replay speed is an input setting, not a measured throughput result. | Pipeline dashboard; timestamped run evidence |
| **Volume** | The batch owner's local report lists 5,325,712 trades for BTCUSDT over two days. The full checkpoint target remains 30 days x 10 symbols trades + 2 years x 50 symbols 1m klines; target size is not an achieved cluster measurement. | [data.md](data.md); local report not rerun by serving work |
| **Variety** | WebSocket JSON (2 exchanges, different schemas), REST JSON (CoinGecko metadata, Fear & Greed index), zipped CSV archives with a timestamp unit change (ms -> us in 2025) | [data.md](data.md) |

## 2. Lambda overview

```mermaid
flowchart LR
  subgraph Sources
    BWS[Binance WebSocket<br/>trade + kline_1m]
    CWS[Coinbase WebSocket<br/>market_trades]
    CG[CoinGecko REST]
    FG[Fear & Greed REST]
    ARC[data.binance.vision<br/>zipped CSV archive]
  end

  subgraph Ingest["Ingest (ns ingest)"]
    PB[producer-binance]
    PC[producer-coinbase]
    PG[poller CronJobs]
    DL[crypto backfill download]
  end

  subgraph Kafka["Kafka / Strimzi (ns kafka)"]
    T1[(raw.trades)]
    T2[(raw.klines)]
    T3[(ref.asset_meta<br/>ref.fear_greed)]
    DLQ[(dlq.ingest)]
  end

  subgraph Speed["Speed layer"]
    SO[stream_ohlcv<br/>Structured Streaming]
  end

  subgraph Batch["Batch layer"]
    SB[stream_bronze]
    BR[(HDFS bronze)]
    BLA[batch_load_archive]
    BS[batch_silver]
    SI[(HDFS silver)]
    GO[(HDFS gold<br/>planned B2)]
  end

  subgraph Serving
    CA[(Cassandra<br/>ohlcv_1m, latest_price)]
    GR[Grafana]
    ST[Streamlit<br/>planned B4]
  end

  BWS --> PB --> T1 & T2
  CWS --> PC --> T1
  CG & FG --> PG --> T3
  PB & PC & PG -. invalid .-> DLQ
  ARC --> DL --> BLA --> BR
  T1 --> SO --> CA
  T1 & T2 --> SB --> BR
  BR --> BS --> SI --> GO -. batch views B2 .-> CA
  CA --> GR
  CA -.-> ST
  GO -.-> ST
```

* **Speed layer**: `stream_ohlcv` validates trades, deduplicates with a 2-minute watermark, then commits an
  internal Parquet handoff. Independent consumers update 1-minute Cassandra candles (30-second watermark),
  update latest prices with event-time write timestamps, and append finalized candles to HDFS gold. Default
  triggers are 10 seconds; latency includes multiple queries and must be measured. Finalization requires
  incoming event time to advance.
* **Batch layer**: `stream_bronze` lands every Kafka record unchanged in HDFS (the immutable master dataset);
  `batch_load_archive` adds years of history from the Binance archive with the same bronze columns;
  `batch_silver` rebuilds cleaned, deduplicated, typed tables hourly. Gold tables and batch views follow in B2.
* **Serving layer**: Cassandra (query-first tables, partition = symbol x day) behind Grafana. The signed
  Cassandra datasource reads bounded date partitions; market ranges are limited to 24 hours. The current
  candle is provisional. Latest price displays its event timestamp independently of the time picker.
* **Monitoring**: Prometheus scrapes annotated producer pods, the driver UI's PrometheusServlet and Strimzi's
  Kafka Exporter. Grafana's separate pipeline dashboard does not treat missing series as zero. Kafka Exporter
  lag covers committed consumer-group offsets; Spark's checkpoint offsets need separate backlog analysis.

Why Lambda and not Kappa: the batch layer can rebuild every view from the immutable bronze data and years of
archive history, which a single replayed stream cannot do cheaply.

## 3. One trade, end to end

```mermaid
sequenceDiagram
  autonumber
  participant EX as Binance WS
  participant P as producer-binance
  participant K as Kafka raw.trades
  participant SO as stream_ohlcv
  participant SB as stream_bronze
  participant H as HDFS
  participant BS as batch_silver
  participant C as Cassandra
  participant G as Grafana

  EX->>P: {"e":"trade","t":5205379727,"T":1756684800178,...}
  Note over P: validate with pydantic Trade<br/>invalid -> dlq.ingest [D1]
  Note over P: reconnect: REST historicalTrades<br/>fromId=last+1 fills the gap [D2: overlap possible]
  P->>K: key=BTC-USDT, idempotent producer, acks=all [D3]
  par speed layer
    K->>SO: micro-batch every 10 s
    Note over SO: validate -> quarantine/ [D1]<br/>dedupe watermark 2 min [D4] -> durable Parquet handoff<br/>separate candle query: watermark 30 s [L1]<br/>window 1 min, open/close by min_by/max_by(event_ts,trade_id) [L2]
    SO->>C: foreachBatch upsert ohlcv_1m by PK [D5]<br/>latest_price with writetime = event time [L3]
    C->>G: CQL query every 30 s
  and batch layer
    K->>SB: micro-batch every 1 min
    SB->>H: bronze/trades/date=/hour= (append, exactly-once file sink) [D6]
    H->>BS: hourly, last 2 days
    Note over BS: validate -> quarantine/<br/>dropDuplicates(exchange,symbol,trade_id) [D7]<br/>late trades included: no watermark in batch [L4]
    BS->>H: silver/trades/date= (dynamic partition overwrite) [D8]
  end
```

Where duplicates (D) and late data (L) are handled:

| # | Place | Mechanism |
|---|---|---|
| D1 | producer / stream / batch | schema + quality rules; rejects go to `dlq.ingest` or `quarantine/` with a reason, never silently dropped |
| D2 | producer after reconnect | gap backfill may re-send trades already sent; tolerated, removed by D4/D7 |
| D3 | producer -> Kafka | `enable.idempotence=true`: broker discards retried batches |
| D4 | stream_ohlcv | `dropDuplicatesWithinWatermark` on the business key, state bounded by the watermark |
| D5 | stream_ohlcv -> Cassandra | upsert by primary key: retry rewrites cumulative candle values; updated_at/TTL can refresh; sinks are independent |
| D6 | stream_bronze | file sink + checkpoint: a batch is committed to `_spark_metadata` exactly once |
| D7 | batch_silver | `dropDuplicates` over all bronze sources (stream + archive overlap) |
| D8 | batch_silver | dynamic partition overwrite: reruns replace partitions instead of appending |
| L1 | stream_ohlcv | trades older than the 30 s watermark are dropped from the live candle (measured in B3) |
| L2 | ohlcv transform | open/close chosen by event time, so out-of-order arrival inside the window is harmless |
| L3 | latest_price | Cassandra write timestamp = event time: an older price can never overwrite a newer one |
| L4 | batch_silver | the batch view includes every late trade; it is the source of truth for history |

## 4. Deployment (kind, local profile)

```mermaid
flowchart TB
  subgraph kind["kind cluster: control-plane + 2 workers"]
    subgraph ns_ingest["ns ingest"]
      D1[Deployment producer-binance]
      D2[Deployment producer-coinbase]
      CJ[CronJobs coingecko, coingecko-categories, fear-greed]
    end
    subgraph ns_kafka["ns kafka"]
      SO1[Strimzi operator]
      KB[KafkaNodePool dual-role x1<br/>PVC 20Gi]
      EO[Entity operator + Kafka Exporter]
    end
    subgraph ns_storage["ns storage"]
      NN[StatefulSet hdfs-namenode x1<br/>PVC 2Gi, Svc :8020 :9870]
      DN[StatefulSet hdfs-datanode x2<br/>headless Svc, PVC 40Gi each]
      CS[StatefulSet cassandra x1<br/>PVC 10Gi, Svc :9042]
      SJ[Job cassandra-schema]
    end
    subgraph ns_spark["ns spark"]
      OP[spark-operator + webhook]
      A1[SparkApplication stream-bronze<br/>driver + 1 executor]
      A2[SparkApplication stream-ohlcv<br/>driver + 1 executor]
      A3[ScheduledSparkApplication batch-silver<br/>hourly]
    end
    subgraph ns_mon["ns monitoring"]
      PR[Prometheus]
      GF[Grafana NodePort 30300<br/>Cassandra plugin]
    end
  end
  D1 & D2 & CJ --> KB
  A1 & A2 --> KB
  A1 & A3 --> NN & DN
  A2 --> CS
  GF --> CS
  PR -. PodMonitors .-> D1 & D2 & A1 & A2 & KB
  laptop[localhost:3000] --> GF
```

Memory budget (requests): Kafka 1 GiB, HDFS 1.8 GiB, Cassandra 1.5 GiB, two streaming apps ~6.5 GiB
(driver 1 GiB + executor 1.5 GiB + overhead each), monitoring ~1 GiB. A laptop needs ~14 GiB for Docker.

## 5. Components

| Component | Technology | Role | Why chosen | Alternative considered |
|---|---|---|---|---|
| Producers | Python 3.11, `websockets`, `confluent-kafka`, pydantic v2 | WS/REST -> validated JSON -> Kafka | asyncio fits long-lived sockets; librdkafka idempotent producer; one model per topic | Kafka Connect HTTP source (no WS support), NiFi (heavy) |
| Message queue | Kafka (KRaft) via Strimzi | durable, replayable buffer between ingest and processing; decouples layers | replay for reprocessing, partitioning by symbol, operator-managed on K8s | Pulsar (smaller ecosystem in course), RabbitMQ (no log replay) |
| Stream processing | Spark Structured Streaming 3.5 | speed layer: OHLCV, dedupe, watermarking | one engine and one codebase for batch and stream; course requirement | Flink (better latency, second engine to learn) |
| Batch processing | Spark SQL / DataFrames | bronze -> silver -> gold, analytics, ML | same transforms as streaming, unit tested with local Spark | Hive on Tez, Trino (query only) |
| Distributed storage | HDFS 3.3 | master dataset, Parquet lake | course focus on distributed FS; data locality; block replication visible | MinIO/S3 |
| Serving DB | Cassandra 5.0.6 | low-latency reads of candles and latest prices | write-heavy time series, query-first modelling | MongoDB, Elasticsearch |
| Orchestration | Kubernetes (kind -> managed), spark-operator | run everything declaratively, restart on failure | same manifests locally and on cloud | Docker Compose |
| Monitoring | kube-prometheus-stack | metrics for producers, Kafka, Spark | standard, PodMonitor CRDs, Grafana included | ELK only (logs, weak metrics) |
| Visualization | Grafana + Cassandra datasource | live candles, pipeline health | no extra service; dashboards as code | Superset (needs SQL engine), Streamlit (planned for analytics) |

## 6. Data contracts

The frozen contracts (symbols, message schemas, Kafka topics, lake layout, Cassandra tables, env vars) live in code:
[contracts.py](../src/cryptopipe/contracts.py), [schemas/](../src/cryptopipe/schemas/), [conf/symbols.yaml](../conf/symbols.yaml),
[cql/schema.cql](../cql/schema.cql) and [config.py](../src/cryptopipe/config.py); `crypto contracts show` prints them.
Summary:

| Topic | Key | Partitions | Value |
|---|---|---|---|
| `raw.trades` | canonical symbol | 12 | `Trade` |
| `raw.klines` | canonical symbol | 6 | `Kline` (closed candles) |
| `ref.asset_meta` | canonical symbol | 1, compacted | `AssetMeta` |
| `ref.fear_greed` | `yyyy-MM-dd` | 1, compacted | `FearGreed` |
| `alerts` | canonical symbol | 3 | `Alert` |
| `dlq.ingest` | source | 3 | `DlqRecord` |

Lake (`/lake`): `bronze/{trades,klines,archive_trades,archive_klines}`, `silver/{trades,klines}`, `gold/...`,
`quarantine/{trades,klines}`, `_checkpoints/`, `_profile/`, partitioned by `date` (and `hour` in stream bronze).
Cassandra (`crypto`): `ohlcv_1m`, `latest_price`, `alerts`, `daily_stats` ([cql/schema.cql](../cql/schema.cql)).

## 7. Planned (not built yet)

| Part | Milestone | Notes |
|---|---|---|
| Gold tables (indicators, pivots), batch views in Cassandra | B2 | window functions, pandas UDFs, join strategies, bucketing |
| Hive Metastore (Postgres) | B2 | the checkpoint uses Spark's in-memory catalog; `silver.trades_bucketed` is re-registered by each run |
| Stateful detection, more output modes, exactly-once drill | B3 | `applyInPandasWithState`, alerts topic |
| MLlib, KMeans, GraphFrames, statistics, Streamlit | B4 | GraphFrames jar is already in the Spark image |
| Avro + Schema Registry | B1 | JSON with `schema_version` until then |
| Airflow or justified ScheduledSparkApplication, alerting, Loki | B5 | |
| Security: Kafka TLS + SCRAM, NetworkPolicies, Cassandra auth, HDFS permissions review | B5 | Kafka checkpoint uses PLAINTEXT; local Cassandra has no auth/TLS; verify HDFS settings separately |
| Managed cloud cluster, 3x replication, load and failure tests | B6 | |
