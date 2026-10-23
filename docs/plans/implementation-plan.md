# Implementation Plan for Claude Code: Crypto Market Data Pipeline

This file is written to be handed to Claude Code (CLI or cloud) as the working spec. It covers:

- **Part A**: the checkpoint next week (git repo with executable code, data ready, system architecture ready). This part is detailed task by task.
- **Part B**: the remaining milestones after the checkpoint, at a coarser level.

The overall project plan, with the reasons behind each choice, is in [crypto-capstone-plan.md](crypto-capstone-plan.md). The comparison with the reference repo is in section 10 of that file.

---

## 0. How to use this file with Claude Code

1. Create an empty GitHub repo (suggested name `crypto-stream-lake`; avoid reusing the reference project's name).
2. Copy this file into the repo as `docs/implementation-plan.md` and create `CLAUDE.md` from section 2 below.
3. Give Claude Code one task at a time, for example: *"Implement task A3 from docs/implementation-plan.md. Follow CLAUDE.md. Stop when its acceptance checks pass."*
4. Each task has **Deliverables** (files to create), **Spec** (what the code must do) and **Done when** (commands that must succeed). Claude Code should run the "Done when" checks itself and commit after each task on a branch, then open a PR.
5. Tasks marked 🖥️ need a running Kubernetes cluster (kind on a laptop with Docker, about 16 GB RAM free). A cloud Claude Code session usually cannot run kind, so do those tasks on your own machine. Tasks without 🖥️ only need Python + Java and can run anywhere.

---

## 1. Fixed technical decisions (do not re-debate during implementation)

| Area | Choice | Pin |
|---|---|---|
| Language | Python 3.11 | |
| Processing | PySpark 3.5.x (Scala 2.12 build) | `pyspark==3.5.*` |
| Spark connectors | `spark-sql-kafka-0-10_2.12`, `spark-cassandra-connector_2.12` (3.5.x), `graphframes` (0.8.x/0.9.x for Spark 3.5) | baked into the Spark image |
| Message queue | Kafka in KRaft mode, deployed with the **Strimzi** operator | current Strimzi release |
| Distributed storage | **HDFS 3.3.x** (`apache/hadoop` image), own manifests: 1 NameNode + 2 DataNodes locally, 3 on cloud | |
| Serving DB | **Cassandra 5.0** (official image) as a StatefulSet; 1 node locally, 3 on cloud | |
| Spark on K8s | **kubeflow spark-operator** Helm chart (`SparkApplication`, `ScheduledSparkApplication`) | |
| Monitoring | `kube-prometheus-stack` Helm chart (Prometheus + Grafana) | |
| Visualization | Grafana with the Cassandra datasource plugin (`hadesarchitect-cassandra-datasource`) | |
| Kafka client | `confluent-kafka` (idempotent producer, `acks=all`) | |
| WebSocket client | `websockets` (asyncio) | |
| Message format (checkpoint) | JSON validated by **pydantic v2** models, one model per topic. Avro + Schema Registry is a planned upgrade (task B1.2), and the migration becomes a "schema versioning" lesson | |
| Config | `pydantic-settings`, every endpoint/credential from env vars, **nothing hardcoded** | |
| CLI | one entrypoint `crypto` (Typer), e.g. `crypto ingest binance`, `crypto backfill`, `crypto replay` | |
| Tests | `pytest`, `chispa` for DataFrame equality, local `SparkSession` fixture | |
| Lint | `ruff` | |
| Local cluster | `kind` (1 control plane + 2 workers) | |

---

## 2. CLAUDE.md to put in the repo root

```markdown
# Project rules

- Python 3.11, PySpark 3.5. Package code lives in `src/cryptopipe/`. Spark jobs are thin `main()` wrappers around pure functions
  (`DataFrame -> DataFrame`) in `src/cryptopipe/transforms/` so they can be unit tested without Kafka/HDFS/Cassandra.
- Never hardcode hosts, ports, paths or secrets. Read them from `cryptopipe.config.Settings` (env vars, `.env` for local).
- Every Kafka message type has a pydantic model in `src/cryptopipe/schemas/`. Producers validate before sending;
  invalid records go to the `dlq.ingest` topic with an error reason.
- Event time is the exchange timestamp (`event_ts`, UTC, milliseconds). Ingestion time is `ingest_ts`. Never mix them.
- Dedupe key for trades: `(exchange, symbol, trade_id)`.
- Spark config lives in `deploy/spark/*.yaml` (SparkApplication) and `conf/spark-defaults.conf`, not in code,
  except settings a job truly needs (e.g. watermark length passed as an argument).
- Every new transform gets a unit test in `tests/unit/`. Run `make lint test` before committing.
- K8s manifests go in `deploy/`. Use namespaces: `kafka`, `storage` (HDFS, Cassandra), `spark`, `monitoring`, `ingest`.
- When something breaks in an instructive way (or an experiment produces numbers), append an entry to
  `docs/journal/<category>.md`: date, symptom, what was tried, numbers, fix. These feed the report's Lessons Learned.
- Commit messages: conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`).
```

---

## 3. Target repository layout

```
crypto-stream-lake/
├── CLAUDE.md
├── README.md                    # quickstart: make cluster-up → make deploy → make demo
├── Makefile
├── pyproject.toml               # package `cryptopipe`, entrypoint `crypto`
├── .env.example
├── .github/workflows/ci.yml     # ruff + pytest
├── conf/
│   ├── symbols.yaml             # tracked symbols per exchange + canonical mapping
│   └── spark-defaults.conf
├── src/cryptopipe/
│   ├── config.py
│   ├── cli.py
│   ├── schemas/                 # pydantic models: trade.py, kline.py, asset_meta.py, fear_greed.py
│   ├── ingest/
│   │   ├── kafka_io.py          # producer factory, delivery callbacks, DLQ helper
│   │   ├── binance_ws.py
│   │   ├── coinbase_ws.py
│   │   ├── coingecko.py         # metadata poller
│   │   ├── fear_greed.py
│   │   ├── archive.py           # data.binance.vision downloader + checksum
│   │   └── replay.py            # archive → Kafka at N× speed
│   ├── transforms/              # pure DataFrame functions (unit tested)
│   │   ├── normalize.py         # symbol mapping, type casting
│   │   ├── dedupe.py
│   │   ├── ohlcv.py
│   │   └── quality.py           # validation rules, returns (good_df, bad_df)
│   ├── jobs/
│   │   ├── stream_bronze.py     # Kafka → HDFS bronze (append)
│   │   ├── stream_ohlcv.py      # Kafka → 1m OHLCV → Cassandra (+ HDFS)
│   │   ├── batch_load_archive.py# archive CSVs → bronze Parquet
│   │   └── batch_silver.py      # bronze → silver (clean, dedupe, normalize)
│   └── common/spark.py          # SparkSession builder reading conf
├── tests/
│   ├── unit/                    # transforms, schemas, producers (mock Kafka)
│   ├── fixtures/                # tiny CSV/JSON samples (committed)
│   └── integration/             # needs cluster; marked @pytest.mark.integration
├── docker/
│   ├── spark/Dockerfile         # apache/spark:3.5.x-python3 + jars + cryptopipe wheel
│   └── ingest/Dockerfile        # python:3.11-slim + cryptopipe
├── deploy/
│   ├── kind/cluster.yaml
│   ├── kafka/                   # Strimzi Kafka, KafkaNodePool, KafkaTopic CRs
│   ├── hdfs/                    # namenode/datanode StatefulSets, configmap (core-site, hdfs-site)
│   ├── cassandra/               # StatefulSet, Service, schema Job (CQL)
│   ├── spark/                   # spark-operator values, SparkApplication YAMLs, RBAC
│   ├── ingest/                  # Deployments for producers, CronJob for pollers
│   ├── monitoring/              # kube-prometheus-stack values, Grafana dashboards (JSON in ConfigMaps)
│   └── scripts/                 # cluster-up.sh, deploy-all.sh, port-forward.sh, smoke-test.sh
├── cql/schema.cql
└── docs/
    ├── architecture.md          # diagrams (Mermaid) + component table
    ├── data.md                  # sources, volumes, HDFS layout, topics, Cassandra tables
    ├── adr/                     # 0001-lambda.md, 0002-hdfs.md, 0003-cassandra.md, 0004-k8s.md ...
    ├── journal/                 # one file per lessons-learned category
    └── checkpoint-1.md          # what is demoed and how
```

---

## Part A: Checkpoint (next week)

**Checkpoint definition of done**

1. Repo on GitHub: README quickstart, CI green, `make test` passes.
2. Executable code: Binance (and Coinbase) producers running in K8s and writing to Kafka; a Spark streaming job writing bronze Parquet to HDFS and 1-minute OHLCV to Cassandra; a batch job loading historical archive data into HDFS bronze/silver.
3. Data ready: at least **30 days of Binance trades for 10 symbols plus 2 years of 1m klines for 50 symbols** in HDFS (several GB), plus live data accumulating; `docs/data.md` lists sizes and row counts.
4. Architecture ready: `docs/architecture.md` with the Lambda diagram, data flow, deployment diagram, component table; ADRs for the main choices; topic, HDFS and Cassandra schemas documented.
5. A Grafana dashboard showing live 1m candles from Cassandra.

Suggested order: about 7 working days. For a team of 5, use the split below.

### Team split for 5 people

Split mostly by layer, but **agree on the contracts together first**. Layers depend on each other (streaming needs the topic schema, the dashboard needs the Cassandra table), so the split only stays independent if the interfaces are fixed on day 1 and everyone builds against fixtures until the cluster is up.

**Before the split:** run [bootstrap-plan.md](bootstrap-plan.md) with Claude Code (steps S1–S10). It covers A1, A2, the contracts, stubs, fixtures and a runnable local demo, so the day-1 work below becomes a review of what the bootstrap produced.

**Day 1, everyone together (2–3 hours):** agree on and commit `docs/contracts.md`: Kafka topic names, keys and message fields (A2), HDFS paths, Cassandra tables (`cql/schema.cql`), and env var names (`.env.example`). Member E pushes the repo skeleton (A1) the same morning so everyone branches from it.

| Member | Role | Tasks | Delivers for the checkpoint | Works independently using |
|---|---|---|---|---|
| **A** | Platform | A7 (kind cluster, Strimzi Kafka + topics, HDFS, Spark Operator + RBAC), Dockerfiles, `make cluster-up deploy`, `smoke-test.sh` | Cluster comes up from one command; topics and HDFS ready | Nothing; others test against this cluster from about day 3 |
| **B** | Ingestion | A2 schemas (day 1), A3 producers (Binance, Coinbase, CoinGecko, Fear & Greed), DLQ, producer metrics, ingest Deployments/CronJobs | Live trades flowing into Kafka | `--dry-run` to stdout, recorded WS fixtures, mock producer in tests |
| **C** | Speed layer | A5 transforms (normalize, quality, dedupe, OHLCV) + tests, `stream_bronze`, `stream_ohlcv`, their `SparkApplication` YAMLs | Streaming jobs writing bronze to HDFS and 1m candles to Cassandra | Local Spark + JSON fixtures; the rate source or a file stream in place of Kafka |
| **D** | Batch layer and data | A4 archive downloader + replayer, `batch_load_archive`, `batch_silver` (bucketed table), A8 dataset load + profile job, `docs/data.md` | 30 days of trades + 2 years of klines in HDFS, with counts and sizes | Local downloads and local Spark; puts files into HDFS once A's cluster is up |
| **E** | Serving, monitoring and docs | A1 repo + CI (day 1), Cassandra StatefulSet + schema Job, kube-prometheus-stack, A9 Grafana dashboards, A10 architecture doc + ADRs + demo script, A11 journal setup | Dashboards, architecture docs, demo script | Cassandra with hand-inserted sample rows for the dashboards |

Rules that keep it independent:
- Each member owns their folders (`src/cryptopipe/ingest/` for B, `jobs/stream_*` for C, and so on); a change to `docs/contracts.md` needs a PR reviewed by the members it affects.
- Everyone writes their own section of `docs/architecture.md` and their own ADR; E assembles and edits.
- Everyone adds at least one journal entry in their area before the checkpoint.

**Integration:** day 5 everyone runs end to end on A's cluster (B's producers → Kafka → C's jobs → Cassandra → E's dashboard; D loads history). Day 6 is for fixing what broke and loading the full dataset. Day 7 is a demo rehearsal with `docs/checkpoint-1.md`.

After the checkpoint, keep the same owners: B adds Avro and the circuit breaker, C adds stateful alerts and the exactly-once test, D adds gold tables, joins and optimizations, E adds Streamlit and monitoring alerts, and A takes security and the cloud deployment. MLlib and GraphFrames (B4) go to whoever finishes first, or are split between C and D.

### A1. Repo bootstrap (day 1)

**Deliverables:** `pyproject.toml`, `Makefile`, `CLAUDE.md`, `.env.example`, `.gitignore` (data dirs, `.env`, `*.parquet`), `src/cryptopipe/{config.py,cli.py,common/spark.py}`, `tests/conftest.py`, `.github/workflows/ci.yml`, `README.md` skeleton.

**Spec**
- `Settings` (pydantic-settings) fields: `kafka_bootstrap`, `kafka_security_protocol`, `hdfs_url` (e.g. `hdfs://hdfs-namenode.storage:8020`), `lake_root` (default `${hdfs_url}/lake`), `cassandra_hosts`, `cassandra_keyspace` (`crypto`), `symbols_file`, `log_level`.
- `common/spark.py::get_spark(app_name, local: bool=False)` builds a SparkSession; with `local=True` uses `local[*]` and a temp warehouse (for tests and laptop runs).
- `Makefile` targets: `install`, `lint`, `test`, `test-integration`, `build-images`, `cluster-up`, `cluster-down`, `deploy`, `demo`, `data-download`.
- `tests/conftest.py`: session-scoped `spark` fixture (`local[2]`, `spark.sql.shuffle.partitions=2`, UTC timezone).
- CI: Python 3.11, Java 17 (`actions/setup-java`), `pip install -e .[dev]`, `ruff check`, `pytest -m "not integration"`.

**Done when:** `make install lint test` passes locally and in CI; `crypto --help` lists command groups.

### A2. Symbol config and message schemas (day 1)

**Deliverables:** `conf/symbols.yaml`, `src/cryptopipe/schemas/*.py`, `tests/unit/test_schemas.py`.

**Spec**
- `symbols.yaml`: canonical symbols (`BTC-USDT`, `ETH-USDT`, …, 10 to 20 entries) with per-exchange names (`binance: BTCUSDT`, `coinbase: BTC-USD`) and the CoinGecko id (`bitcoin`).
- `Trade`: `exchange`, `symbol` (canonical), `trade_id` (str), `price` (Decimal or float, > 0), `qty` (> 0), `side` (`buy`/`sell`, from the taker side), `event_ts` (ms), `ingest_ts` (ms), `schema_version` (int, starts at 1).
- `Kline`: `exchange`, `symbol`, `interval`, `open_ts`, `close_ts`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trades`, `is_closed`.
- `AssetMeta` (from CoinGecko): `coingecko_id`, `symbol`, `name`, `categories`, `market_cap`, `circulating_supply`, `snapshot_ts`.
- `FearGreed`: `value`, `classification`, `ts`.
- A matching Spark `StructType` is generated from each model (helper `to_spark_schema(Model)`), so streaming jobs share one source of truth.

**Done when:** tests cover valid parse, rejected negative price, unknown symbol rejected, Spark schema field names equal model fields.

### A3. Kafka producers (days 2–3)

**Deliverables:** `ingest/kafka_io.py`, `ingest/binance_ws.py`, `ingest/coinbase_ws.py`, `ingest/coingecko.py`, `ingest/fear_greed.py`, CLI commands, unit tests with a fake producer.

**Spec**
- `kafka_io.make_producer()`: `enable.idempotence=true`, `acks=all`, `compression.type=zstd`, `linger.ms=50`. Key = canonical symbol (keeps per-symbol ordering). `send_dlq(raw, reason)` writes to `dlq.ingest`.
- **Binance**: combined stream `wss://stream.binance.com:9443/stream?streams=<sym>@trade/<sym>@kline_1m/...` for all symbols in config. Map to `Trade` → topic `raw.trades`, `Kline` → `raw.klines` (only `is_closed=true`, plus open ones optionally to `raw.klines.live`). Binance closes connections after 24 h, so reconnect proactively at about 23 h and on any error, with exponential backoff (1 s → 60 s cap, jitter).
- **Gap backfill on reconnect**: remember the last `trade_id` per symbol; after reconnect call REST `GET /api/v3/aggTrades` or `/api/v3/historicalTrades` (whichever works without a key; `aggTrades` with `fromId` is keyless) to fill the gap. Log the gap size (journal material for the ingestion lesson).
- **Coinbase**: `wss://advanced-trade-ws.coinbase.com`, `market_trades` channel (public, check current docs for whether a JWT is required; if it is, fall back to Kraken `wss://ws.kraken.com/v2` `trade` channel). Same `Trade` model, `exchange="coinbase"`.
- **CoinGecko** poller: `/coins/markets?vs_currency=usd&ids=...` every 10 minutes (free tier rate limits: keep to < 10 calls/min), topic `ref.asset_meta`.
- **Fear & Greed**: `https://api.alternative.me/fng/?limit=0` once a day, topic `ref.fear_greed`.
- Expose Prometheus metrics from each producer on `:8000/metrics` (`prometheus_client`): messages sent, DLQ count, reconnects, gap trades backfilled, last event lag (`ingest_ts - event_ts`).
- CLI: `crypto ingest binance|coinbase|coingecko|fear-greed`.

**Done when:** unit tests parse recorded sample WS payloads (save 20 real messages per stream into `tests/fixtures/`), and `crypto ingest binance --dry-run` prints validated trades to stdout for 30 s from a laptop.

### A4. Historical archive download and replayer (day 2–3)

**Deliverables:** `ingest/archive.py`, `ingest/replay.py`, `crypto backfill download`, `crypto replay`, `docs/data.md` (first version).

**Spec**
- Download from `https://data.binance.vision/data/spot/daily/trades/<SYMBOL>/<SYMBOL>-trades-<YYYY-MM-DD>.zip` and `.../monthly/klines/<SYMBOL>/1m/...`; verify the `.CHECKSUM` file (SHA256); skip files already present (idempotent, resumable).
- Note: from 2025 onward Binance spot archive timestamps are in **microseconds**; detect by magnitude and normalize to ms. Write that into `docs/data.md` and the ingestion journal.
- Targets: `--symbols`, `--start`, `--end`, `--kind trades|klines`, `--dest` (local dir or `hdfs://` via `hdfs dfs -put` / `pyarrow.fs.HadoopFileSystem`).
- Replayer: reads archive CSVs in timestamp order, rewrites `event_ts` relative to now (optional), produces to `raw.trades` at `--speed 10` (10× real time) or `--rate N msg/s`. Used for load tests and to make the stream busy during the demo.

**Done when:** `crypto backfill download --symbols BTCUSDT --kind trades --start 2025-09-01 --end 2025-09-02 --dest ./data/archive` downloads, verifies and unzips; `crypto replay --path ./data/archive/... --speed 50 --dry-run` prints in order.

### A5. Spark transforms + tests (days 3–4)

**Deliverables:** `transforms/{normalize,dedupe,quality,ohlcv}.py`, tests.

**Spec**
- `normalize.parse_trades(df_raw_json)`: from Kafka `value` (JSON) to typed columns using the shared schema; add `event_time` (timestamp), `date` (string `yyyy-MM-dd`) and `hour`.
- `quality.validate_trades(df) -> (good, bad)`: price > 0, qty > 0, `event_ts` not more than 5 min in the future, symbol in config; `bad` carries `reason`.
- `dedupe.dedupe_trades(df, streaming: bool)`: batch uses `dropDuplicates(["exchange","symbol","trade_id"])`; streaming uses `withWatermark("event_time", "2 minutes").dropDuplicatesWithinWatermark([...])`.
- `ohlcv.ohlcv_1m(df)`: `groupBy(window(event_time, "1 minute"), exchange, symbol)` with open = price of min `event_ts` (use `min_by`/`max_by`), high, low, close, volume, quote volume, trade count, VWAP.

**Done when:** unit tests (chispa) pass for each function with fixture data, including a duplicate trade and an out-of-order trade.

### A6. Spark jobs (days 4–5)

**Deliverables:** `jobs/stream_bronze.py`, `jobs/stream_ohlcv.py`, `jobs/batch_load_archive.py`, `jobs/batch_silver.py`, `cql/schema.cql`.

**Spec**
- `stream_bronze`: Kafka `raw.trades`, `raw.klines` → Parquet at `${lake_root}/bronze/<topic>/date=…/hour=…`, `outputMode("append")`, trigger 1 minute, checkpoint at `${lake_root}/_checkpoints/stream_bronze/<topic>`. Keep the raw JSON plus Kafka `partition`, `offset`, `timestamp` columns for lineage.
- `stream_ohlcv`: Kafka `raw.trades` → parse → validate (bad rows to HDFS `quarantine/`) → dedupe → `ohlcv_1m` with watermark 30 s, `outputMode("update")` → `foreachBatch` writing to Cassandra table `ohlcv_1m` (upsert by primary key, so replays after a crash are idempotent). Checkpoint on HDFS.
- `batch_load_archive`: archive CSVs → bronze Parquet with the same columns as the stream bronze (normalize µs→ms).
- `batch_silver`: bronze (stream + archive) → silver `trades` partitioned by `date`, deduplicated, normalized symbols, sorted within partitions by `symbol, event_ts`. Write it also as a Hive-style table `silver.trades_bucketed` with `bucketBy(16, "symbol")` (needed later for the join/bucketing experiments). Use an embedded Derby or a small Postgres Hive Metastore later; for the checkpoint, Spark's built-in catalog with `spark.sql.warehouse.dir` on HDFS is enough.
- `cql/schema.cql`:
  ```sql
  CREATE KEYSPACE IF NOT EXISTS crypto WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  CREATE TABLE IF NOT EXISTS crypto.ohlcv_1m (
    symbol text, day date, window_start timestamp, exchange text,
    open double, high double, low double, close double,
    volume double, quote_volume double, vwap double, trades int, updated_at timestamp,
    PRIMARY KEY ((symbol, day), window_start, exchange)
  ) WITH CLUSTERING ORDER BY (window_start DESC, exchange ASC)
    AND default_time_to_live = 2592000;  -- 30 days hot data
  CREATE TABLE IF NOT EXISTS crypto.latest_price (
    symbol text, exchange text, price double, event_ts timestamp,
    PRIMARY KEY (symbol, exchange));
  ```

**Done when:** every job runs in local mode against fixture data (`crypto job run batch_silver --local --input tests/fixtures/...`) and an integration test (🖥️) shows rows in Cassandra and Parquet files in HDFS.

### A7. Local Kubernetes platform 🖥️ (days 2–4, in parallel)

**Deliverables:** `deploy/kind/cluster.yaml`, `deploy/{kafka,hdfs,cassandra,spark,monitoring,ingest}/`, `deploy/scripts/*.sh`.

**Spec**
- `cluster-up.sh`: create kind cluster (1 control plane + 2 workers, extraPortMappings for Grafana 3000), create namespaces, install Strimzi, spark-operator and kube-prometheus-stack with Helm, then apply manifests.
- **Kafka** (Strimzi): KRaft `KafkaNodePool` with 1 broker+controller for local (`lite` profile) and 3 for cloud; `KafkaTopic` CRs: `raw.trades` (12 partitions), `raw.klines` (6), `ref.asset_meta` (1, compacted), `ref.fear_greed` (1, compacted), `dlq.ingest` (3). Retention 7 days.
- **HDFS**: ConfigMap with `core-site.xml` and `hdfs-site.xml`; NameNode StatefulSet (format on first start via init container if not formatted), DataNode StatefulSet (2 replicas) with headless Service; set `dfs.client.use.datanode.hostname=true` and `dfs.datanode.use.datanode.hostname=true` so Spark pods reach DataNodes by DNS. PVCs from kind's default storage class. Expected pitfall worth a journal entry: DataNodes registering with pod IPs that change on restart.
- **Cassandra**: StatefulSet (1 replica locally), heap limited (`MAX_HEAP_SIZE=1G`), a Job that waits for CQL and applies `cql/schema.cql`.
- **Spark**: ServiceAccount + RBAC for the driver; `SparkApplication` YAMLs for `stream_bronze` and `stream_ohlcv` (`restartPolicy: Always` for streaming), `ScheduledSparkApplication` for `batch_silver` (hourly). Driver 1 core/1 GB, 1–2 executors 1 core/1.5 GB locally. Enable Prometheus metrics (`spark.ui.prometheus.enabled=true`, `PrometheusServlet`).
- **Ingest**: Deployments for `binance` and `coinbase` producers, CronJobs for `coingecko` (every 10 min) and `fear-greed` (daily). ServiceMonitor for their `/metrics`.
- **Images**: `make build-images` builds `cryptopipe-spark` and `cryptopipe-ingest`, then `kind load docker-image`.
- `smoke-test.sh`: checks all pods Ready, lists Kafka topics with message counts, `hdfs dfs -du -h /lake`, `cqlsh -e "select count(*) from crypto.ohlcv_1m"`.

**Done when:** `make cluster-up deploy` from a clean machine ends with all pods Ready and `smoke-test.sh` showing non-zero counts after 5 minutes.

### A8. Load the checkpoint dataset 🖥️ (days 4–6)

**Spec**
- Download 30 days of trades for 10 symbols and 2 years of 1m klines for 50 symbols (`make data-download`). Start with fewer if bandwidth is a problem and say so in `docs/data.md`.
- Put into HDFS bronze, run `batch_load_archive` and `batch_silver`.
- Generate a data profile report with a small Spark job `crypto job run profile`: row counts per symbol/day, min/max timestamps, null counts, duplicates removed, size on disk (Parquet vs raw CSV ratio). Paste its output into `docs/data.md`.

**Done when:** `docs/data.md` shows real counts and sizes, and `hdfs dfs -ls /lake/silver/trades` shows date partitions.

### A9. Grafana dashboard 🖥️ (day 6)

**Spec**
- Provision the Cassandra datasource and a dashboard JSON (stored in `deploy/monitoring/dashboards/market.json`, loaded via ConfigMap sidecar): candlestick panel for a selected symbol (template variable), latest price table, trades per minute.
- Second dashboard `pipeline.json`: producer messages/s, DLQ count, Kafka consumer lag (from Strimzi's Kafka Exporter), Spark streaming input rate and batch duration.

**Done when:** both dashboards load on `http://localhost:3000` with live data.

### A10. Architecture documentation (days 5–7, can start day 1)

**Deliverables:** `docs/architecture.md`, `docs/adr/*.md`, `docs/data.md`, `docs/checkpoint-1.md`, updated `README.md`.

**Spec for `docs/architecture.md`**
1. Problem summary and 3V numbers (from `docs/data.md`).
2. Lambda overview diagram (Mermaid `flowchart LR`): sources → producers → Kafka → speed layer (Structured Streaming → Cassandra) and batch layer (HDFS bronze/silver/gold → Spark batch → Cassandra batch views) → serving (Grafana, later Streamlit).
3. Data flow sequence diagram for one trade from WebSocket to dashboard, listing each place where duplicates or late data are handled.
4. Deployment diagram: K8s namespaces, StatefulSets/Deployments, Services, PVCs.
5. Component table: component, technology, role, why chosen, alternative considered.
6. Data contracts: Kafka topics (key, partitions, retention, schema), HDFS layout, Cassandra tables.
7. Planned (not yet built) parts marked clearly: MLlib, GraphFrames, Avro/Schema Registry, Airflow, security, cloud deployment.

ADRs (one page each: context, decision, consequences): Lambda vs Kappa; HDFS vs MinIO; Cassandra vs MongoDB/Elasticsearch; Kubernetes (kind → managed) vs Docker Compose; JSON now and Avro later.

`docs/checkpoint-1.md`: a demo script (commands to run, what to show, expected output, screenshots).

**Done when:** diagrams render on GitHub, and the demo script runs start to finish.

### A11. Journal seed (ongoing)

Create `docs/journal/` with one file per category (ingestion, spark-processing, streaming, storage, integration, performance, monitoring, scaling, quality-testing, security, fault-tolerance), each with the lesson template from the course. By the checkpoint, aim for 3 to 5 real entries (likely: Binance µs timestamps, WS reconnect gaps, HDFS DataNode hostname resolution, small files from the streaming sink, kind resource limits).

---

## Part B: After the checkpoint

Each item below should be broken into tasks in the same format (Deliverables / Spec / Done when) at the start of its milestone. Ask Claude Code: *"Expand milestone B2 of docs/implementation-plan.md into tasks in the same format as Part A."*

### B1. Ingestion hardening (week 2)
1. Kafka → HDFS for `ref.*` topics; daily snapshot of asset metadata (slowly changing dimension, keep history).
2. Avro + Schema Registry (Apicurio Registry or Confluent Schema Registry on K8s), `schema_version` 2, compatibility mode BACKWARD; migrate producers and parsers; journal the migration.
3. Circuit breaker + retry policy for REST pollers (`pybreaker`/custom), rate-limit handling (HTTP 429, `Retry-After`).

### B2. Batch layer: gold tables and Spark requirements 1–4 (weeks 2–4)
1. `gold/indicators`: window functions (SMA/EMA, RSI, Bollinger bands, log returns, rolling volatility), `percentile_approx`, `pandas_udf` grouped aggregate for realized volatility.
2. Pivot closes per symbol → correlation input; unpivot back.
3. UDFs: whale classification, cross-exchange symbol normalization; benchmark Python UDF vs `pandas_udf` vs native (record numbers).
4. Joins: broadcast (trades × asset metadata), sort-merge (Binance × Coinbase on symbol+minute for spread), multi-join with Fear & Greed; AQE skew handling; `explain("formatted")` captured into `docs/perf/`.
5. Bucketing vs non-bucketed join benchmark; caching strategies; CBO with `ANALYZE TABLE`; partition pruning proof.
6. Hive Metastore (Postgres-backed) so tables persist across jobs.
7. Batch views to Cassandra (`daily_stats`, `indicators_1h`); serving query merges batch + realtime.

### B3. Speed layer complete (weeks 3–4)
1. Output modes: append (1m OHLCV to HDFS), update (sliding 5m volatility), complete (top movers).
2. Stateful detection with `applyInPandasWithState` (whale bursts, pump-and-dump patterns) → `alerts` topic and Cassandra.
3. Late data measurement and side output; exactly-once test: kill the driver pod during writes, verify no duplicates or gaps in Cassandra and HDFS (script in `tests/integration/test_exactly_once.py`).

### B4. Advanced analytics (weeks 4–5)
1. MLlib pipeline (features from gold indicators + Fear & Greed; `GBTClassifier` for next-interval direction, time-based split, `CrossValidator`), model saved to HDFS, optional streaming scoring.
2. KMeans clustering of assets.
3. GraphFrames: triangular arbitrage motifs, PageRank, connected components on the correlation graph.
4. Statistics: correlation matrix, return distribution, autocorrelation, rolling beta to BTC.
5. Streamlit app (`serving/app.py`) reading Cassandra and gold tables.

### B5. Orchestration, monitoring, security (week 5)
1. Airflow (official Helm chart) with `SparkKubernetesOperator` DAGs for the batch chain, or keep `ScheduledSparkApplication` and justify in an ADR.
2. Alerts: consumer lag, no data for 5 min, streaming batch duration > trigger, pod restarts; Loki for logs.
3. Security: Strimzi TLS + SCRAM users and ACLs, K8s Secrets, NetworkPolicies, Cassandra auth, HDFS permissions, K8s audit logging.

### B6. Cloud deployment and experiments (weeks 6–7)
1. Managed cluster (GKE/EKS/AKS) with 3 to 4 nodes; Kafka 3 brokers RF=3 `min.insync.replicas=2`; HDFS 3 DataNodes; Cassandra 3 nodes RF=3.
2. Load tests with the replayer at increasing rates; scaling experiments (partitions, executors, dynamic allocation, HPA on producers); cost per hour.
3. Fault-tolerance drills: kill a broker, a DataNode, a Cassandra node, the streaming driver; backup/restore (Cassandra snapshots or Medusa, HDFS snapshots).

### B7. Report and demo (week 8 onward)
Write the report from `docs/architecture.md`, `docs/adr/`, `docs/perf/` and `docs/journal/`; record a demo video; tag a release.
