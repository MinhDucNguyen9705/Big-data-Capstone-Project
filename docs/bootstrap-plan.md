# Bootstrap Plan for Claude Code: Shared Foundation Before the Team Splits

> Executed on 2026-10-20 (steps S1 to S9; S10 needs `gh`, see the README). `architecture-overview.md` below
> is [architecture.md](architecture.md) in this repo.

**Goal:** one person (with Claude Code) builds the shared foundation in about one day. After it is merged, the five members (A Platform, B Ingestion, C Speed layer, D Batch and data, E Serving and docs) can work in parallel without waiting on each other.

What the bootstrap produces:

1. A GitHub repo with layout, packaging, CLI, config, lint, tests and CI working.
2. **Frozen contracts**: message schemas, Kafka topics, HDFS layout, Cassandra tables, env vars. These are the interfaces between layers.
3. **Stubs with signatures** for every module each member will own, so imports work and each person fills in their own files.
4. **Shared test fixtures** (small real samples), so every layer can be developed with no cluster.
5. A **walking skeleton**: `crypto demo local` runs fixture trades through parse → validate → dedupe → 1m OHLCV in local Spark and writes Parquet. The repo is executable from day 1.
6. Team workflow files: CODEOWNERS, PR template, issue per task, branch rules.

This file is the spec for Claude Code. Give it the whole file with: *"Execute bootstrap-plan.md steps S1 to S10 in order. Run each step's Done-when checks. Commit after each step."* Related files: [implementation-plan.md](implementation-plan.md) (tasks A1–A11 and Part B) and [architecture.md](architecture.md).

The bootstrap does **not** include the Kubernetes cluster, real producers, real Spark jobs or dashboards. Those belong to the members.

---

## Before running Claude Code (team, 30 minutes)

- Create an empty GitHub repo, e.g. `crypto-stream-lake`, and add all five members as collaborators.
- Decide the tracked symbols. Suggested: BTC, ETH, BNB, SOL, XRP, ADA, DOGE, AVAX, LINK, LTC against USDT (Binance) and USD (Coinbase).
- Each member installs: Python 3.11, Java 17, git, Docker, kind, kubectl, Helm (A, C, D and E need the last four in a few days).

---

## S1. Repository skeleton

**Create:** the full layout from section 3 of `implementation-plan.md`. Empty folders get a `README.md` saying what goes there and who owns it.

Key files:
- `pyproject.toml`: package `cryptopipe` under `src/`, Python `>=3.11,<3.12`, console script `crypto = cryptopipe.cli:app`.
  - Dependencies: `pyspark>=3.5,<3.6`, `pydantic>=2`, `pydantic-settings`, `typer`, `confluent-kafka`, `websockets`, `httpx`, `prometheus-client`, `pyyaml`, `cassandra-driver`.
  - Extra `dev`: `pytest`, `pytest-asyncio`, `chispa`, `ruff`, `pre-commit`.
- `.gitignore`: `.env`, `data/`, `*.parquet`, `spark-warehouse/`, `metastore_db/`, `derby.log`, `.venv/`, `__pycache__/`, `checkpoints/`.
- `.python-version` (`3.11`), `.editorconfig`, `.pre-commit-config.yaml` (ruff, ruff-format, end-of-file, trailing whitespace).
- `LICENSE` (MIT unless the course says otherwise).

**Done when:** `python -m venv .venv && pip install -e .[dev]` succeeds; `crypto --help` runs.

## S2. Config and Spark session

**Create:** `src/cryptopipe/config.py`, `src/cryptopipe/common/spark.py`, `src/cryptopipe/common/logging.py`, `.env.example`, `conf/spark-defaults.conf`.

- `Settings` (pydantic-settings, prefix `CRYPTO_`). Each field has a local default so tests run with no `.env`:

  | Env var | Default (local) | Used by |
  |---|---|---|
  | `CRYPTO_KAFKA_BOOTSTRAP` | `localhost:9092` | B, C |
  | `CRYPTO_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | B, C |
  | `CRYPTO_LAKE_ROOT` | `file:///tmp/crypto-lake` (cluster: `hdfs://hdfs-namenode.storage:8020/lake`) | C, D |
  | `CRYPTO_CHECKPOINT_ROOT` | `${LAKE_ROOT}/_checkpoints` | C |
  | `CRYPTO_CASSANDRA_HOSTS` | `localhost` | C, D, E |
  | `CRYPTO_CASSANDRA_KEYSPACE` | `crypto` | C, D, E |
  | `CRYPTO_SYMBOLS_FILE` | `conf/symbols.yaml` | all |
  | `CRYPTO_LOG_LEVEL` | `INFO` | all |
  | `CRYPTO_METRICS_PORT` | `8000` | B |

- `get_spark(app_name, local=False)`: in local mode uses `local[*]`, UTC session timezone, `spark.sql.shuffle.partitions=4` and a temp warehouse. In cluster mode reads everything from `spark-defaults.conf` / SparkApplication config. Connector jars come from the image, not from code.
- JSON logging helper used by producers and jobs.

**Done when:** `python -c "from cryptopipe.config import settings; print(settings)"` prints defaults; a unit test creates a local Spark session.

## S3. Contracts (the most important step)

**Create:** `conf/symbols.yaml`, `src/cryptopipe/schemas/{trade,kline,asset_meta,fear_greed,alert,common}.py`, `src/cryptopipe/contracts.py`, `cql/schema.cql`, `docs/contracts.md`.

- **Symbols**: canonical id `BTC-USDT` plus `binance: BTCUSDT`, `coinbase: BTC-USD`, `coingecko: bitcoin`. A helper `SymbolMap` gives exchange ↔ canonical lookups.
- **Pydantic models** (exact fields in A2 of `implementation-plan.md`), each with `schema_version: int = 1`. Add `Alert` (`alert_id`, `symbol`, `type`, `severity`, `value`, `threshold`, `event_ts`, `details`).
- **`to_spark_schema(Model) -> StructType`** helper, unit-tested, so Spark jobs reuse the same definitions.
- **`contracts.py`** holds the constants every layer imports, so nobody hardcodes strings:
  - Topics: `raw.trades` (12 partitions, key = canonical symbol), `raw.klines` (6), `ref.asset_meta` (1, compacted), `ref.fear_greed` (1, compacted), `alerts` (3), `dlq.ingest` (3).
  - Lake paths: `bronze/trades`, `bronze/klines`, `bronze/archive_trades`, `bronze/archive_klines`, `silver/trades`, `silver/klines`, `gold/...`, `quarantine/...`, partition columns `date` and `hour`.
  - Cassandra tables: `ohlcv_1m`, `latest_price`, `alerts`, `daily_stats`.
- **`cql/schema.cql`**: tables from A6 plus `alerts` (`PRIMARY KEY ((symbol, day), event_ts, alert_id)`) and `daily_stats` (`PRIMARY KEY (symbol, day)`).
- **`docs/contracts.md`**: a human-readable version of all of the above, with an example message per topic, and a rule at the top: *changes need a PR approved by the owners of every affected layer.*

**Done when:** schema tests pass; the generated Spark schemas match the model fields; `docs/contracts.md` has an example for each topic.

## S4. Shared fixtures

**Create:** `tests/fixtures/` with small, real samples (commit them; total < 2 MB):
- `binance_ws_trade.jsonl`, `binance_ws_kline.jsonl`: 50 raw messages each (Claude Code may capture them with a 20-second script against the public stream; if the network blocks it, hand-write them from Binance docs examples).
- `coinbase_ws_trade.jsonl`: 50 raw messages (same approach).
- `trades_normalized.jsonl`: 500 trades in the contract format for 3 symbols over 10 minutes, **including 5 exact duplicates, 5 out-of-order trades, 3 late trades and 3 invalid rows** (negative price, unknown symbol, future timestamp). Generate with a seeded script `tests/fixtures/make_fixtures.py` so the file can be rebuilt.
- `archive_trades_sample.csv`: 1,000 rows in Binance archive format (headerless columns: id, price, qty, quote_qty, time, is_buyer_maker, is_best_match), with **microsecond** timestamps.
- `coingecko_markets.json`, `fear_greed.json`: one real response each.
- `expected_ohlcv_1m.json`: expected candles for `trades_normalized.jsonl`, computed by hand or by a pandas reference in the generator script.

**Done when:** `python tests/fixtures/make_fixtures.py --check` confirms the files match the seed.

## S5. Module stubs with signatures

**Create** every owned module with docstrings, typed signatures and `raise NotImplementedError("Task A3 – owner B")`. Each stub file starts with a header comment naming its owner and task. Examples:

| File | Owner | Key signatures |
|---|---|---|
| `ingest/kafka_io.py` | B | `make_producer(settings) -> Producer`, `send(producer, topic, model)`, `send_dlq(producer, raw, reason)` |
| `ingest/binance_ws.py` | B | `async run(symbols, producer, dry_run=False)`, `parse_trade(msg) -> Trade`, `parse_kline(msg) -> Kline` |
| `ingest/coinbase_ws.py` | B | `async run(...)`, `parse_trade(msg) -> Trade` |
| `ingest/coingecko.py`, `ingest/fear_greed.py` | B | `fetch() -> list[Model]`, `run_once(producer)` |
| `ingest/archive.py` | D | `download(symbols, kind, start, end, dest) -> list[Path]`, `verify_checksum(path)` |
| `ingest/replay.py` | D | `replay(path, producer, speed=10.0, rate=None, dry_run=False)` |
| `transforms/normalize.py` | C | `parse_trades(df_kafka) -> DataFrame` |
| `transforms/quality.py` | C | `validate_trades(df) -> tuple[DataFrame, DataFrame]` |
| `transforms/dedupe.py` | C | `dedupe_trades(df, streaming: bool) -> DataFrame` |
| `transforms/ohlcv.py` | C | `ohlcv_1m(df) -> DataFrame` |
| `transforms/archive.py` | D | `archive_trades_to_contract(df_csv, symbol) -> DataFrame` (µs → ms) |
| `jobs/stream_bronze.py`, `jobs/stream_ohlcv.py` | C | `main(argv)` |
| `jobs/batch_load_archive.py`, `jobs/batch_silver.py`, `jobs/profile.py` | D | `main(argv)` |
| `serving/cassandra_io.py` | E | `write_ohlcv(df_batch, batch_id)` for `foreachBatch`, `get_session(settings)` |

**Important exception:** implement `parse_trades`, `validate_trades`, `dedupe_trades(streaming=False)` and `ohlcv_1m` minimally in the bootstrap (they are needed for the walking skeleton in S6). C later hardens them (streaming dedupe, VWAP edge cases, more rules).

Tests for unimplemented stubs live in `tests/unit/<area>/` and are marked `@pytest.mark.skip(reason="TODO A3")`, so each owner just removes the marker.

**Done when:** `python -c "import cryptopipe.ingest.binance_ws, cryptopipe.jobs.stream_ohlcv"` (and the others) imports cleanly; `pytest` shows skips, not errors.

## S6. Walking skeleton (local end to end)

**Create:** `src/cryptopipe/demo/local.py`, CLI command `crypto demo local`.

- Read `tests/fixtures/trades_normalized.jsonl` as if it were the Kafka `value` column → `parse_trades` → `validate_trades` → `dedupe_trades` → `ohlcv_1m`.
- Write bronze, quarantine and OHLCV Parquet under `CRYPTO_LAKE_ROOT` (local `file://` path by default), partitioned by `date`.
- Print a summary: rows in, invalid rows, duplicates removed, candles out, and the first 5 candles.
- Test `tests/unit/test_demo_local.py` compares the output with `expected_ohlcv_1m.json` using chispa.

**Done when:** `crypto demo local` prints the summary in under a minute on a laptop, and the test passes.

## S7. CLI wiring

**Create:** `src/cryptopipe/cli.py` with Typer groups so every member's entrypoint already exists:
```
crypto ingest binance|coinbase|coingecko|fear-greed [--dry-run]
crypto backfill download --symbols ... --kind trades|klines --start --end --dest
crypto replay --path ... [--speed N | --rate N] [--dry-run]
crypto job run <name> [--local] [job args...]
crypto demo local
crypto contracts show        # prints topics, paths and tables from contracts.py
```
Unimplemented commands exit with a clear "not implemented yet (task A3, owner B)" message and code 2.

**Done when:** `crypto contracts show` works and every command shows help.

## S8. Make targets, CI and Docker image stubs

- `Makefile`: `install`, `lint`, `fmt`, `test`, `test-integration`, `demo`, `fixtures`, plus placeholders for A's targets (`build-images`, `cluster-up`, `cluster-down`, `deploy`, `smoke`) that print "owner A".
- `.github/workflows/ci.yml`: on push and PR: setup Python 3.11 and Java 17 (temurin), cache pip, `make install lint test`, then `crypto demo local`.
- `docker/spark/Dockerfile` and `docker/ingest/Dockerfile` as working first drafts (A finalizes): the Spark image from `apache/spark:3.5.x-python3` with the Kafka and Cassandra connector jars downloaded at build time and the `cryptopipe` wheel installed; the ingest image from `python:3.11-slim`.

**Done when:** CI is green on GitHub; `docker build -f docker/ingest/Dockerfile .` succeeds where Docker is available.

## S9. Docs and team workflow

**Create:**
- `README.md`: project summary, the architecture image or Mermaid diagram from `architecture-overview.md`, quickstart (`make install`, `make test`, `crypto demo local`), team table with owners, links to docs.
- `CLAUDE.md`: the rules from section 2 of `implementation-plan.md`, plus: *"Only edit files your task owns; contracts change only through a PR that updates `docs/contracts.md`."*
- `docs/architecture.md`: copy of `architecture-overview.md` (E maintains it).
- `docs/adr/0000-template.md` and `docs/adr/0001-lambda-architecture.md` (filled in, since it is already decided).
- `docs/journal/<category>.md` × 11 with the course's lesson template.
- `docs/checkpoint-1.md`: definition of done from Part A, with an empty checklist per member.
- `.github/CODEOWNERS`: map folders to members (fill GitHub handles), e.g. `src/cryptopipe/ingest/ @memberB`, `src/cryptopipe/transforms/ @memberC`, `deploy/ @memberA`, `docs/ @memberE`, `src/cryptopipe/contracts.py @memberA @memberB @memberC @memberD @memberE`.
- `.github/pull_request_template.md`: what changed, how tested, task id, journal entry added (yes/no).
- `CONTRIBUTING.md`: branch naming `<member>/<task-id>-<short-name>` (e.g. `b/a3-binance-producer`); one task per PR; squash merge; at least one review; CI must pass.

**Done when:** the README quickstart works from a fresh clone.

## S10. Issues and project board (optional, needs `gh` authenticated)

Create one GitHub issue per task in Part A (A2–A11) and Part B milestones, labelled with `owner:A` to `owner:E`, `checkpoint-1` or `later`, each issue body copying its Spec and Done-when from `implementation-plan.md`. Create a milestone "Checkpoint 1" with the checkpoint date.

**Done when:** `gh issue list --milestone "Checkpoint 1"` shows the checkpoint tasks.

---

## Bootstrap done = handoff checklist

- [ ] Fresh clone → `make install test` green, `crypto demo local` prints candles.
- [ ] CI green on `main`; branch protection on `main` (require PR + CI) set in GitHub settings.
- [ ] `docs/contracts.md` reviewed by all five members in a short meeting, then frozen for the checkpoint.
- [ ] Every member can run the tests and the demo on their own machine.
- [ ] Issues assigned.

---

## What happens after the bootstrap

### Days 2–4: independent work (each in their own folders)

| Member | First tasks | Can test without others by |
|---|---|---|
| A Platform | kind cluster, Strimzi Kafka + topics from `contracts.py`, HDFS, Spark Operator, final images, `make cluster-up deploy smoke` | Deploying the images built from the bootstrap |
| B Ingestion | Binance + Coinbase producers, CoinGecko and Fear & Greed pollers, DLQ, metrics | `--dry-run` and the WS fixtures |
| C Speed layer | Harden transforms, `stream_bronze`, `stream_ohlcv` with watermark, streaming dedupe, `foreachBatch` to Cassandra | Spark file stream over the fixture JSONL instead of Kafka |
| D Batch and data | Archive downloader, replayer, `batch_load_archive`, `batch_silver` with bucketing, profile job, start the big download early | Local files and local Spark |
| E Serving and docs | Cassandra on kind + schema Job, kube-prometheus-stack, Grafana dashboards, architecture doc and ADRs | Cassandra with sample rows inserted from the fixtures |

Daily: a 15-minute sync; anyone who needs a contract change opens a PR that day.

### Day 5: first integration on the cluster
B's producers → Kafka → C's streaming jobs → HDFS + Cassandra → E's dashboards, with D loading history into HDFS. Expect breakages (DNS, jars, HDFS hostnames); each one becomes a journal entry.

### Day 6: full data load and fixes
D loads the full checkpoint dataset and fills in `docs/data.md`; everyone fixes integration bugs.

### Day 7: checkpoint rehearsal
Run `docs/checkpoint-1.md` start to finish from a clean cluster, take screenshots, tag `v0.1-checkpoint`.

### After the checkpoint (same owners, from Part B of `implementation-plan.md`)
- **A:** security (Kafka TLS/SCRAM/ACLs, RBAC, NetworkPolicies), cloud cluster, fault-tolerance drills, scaling and cost experiments.
- **B:** Avro + Schema Registry migration (schema version 2), circuit breaker and rate-limit handling, metadata snapshots to HDFS.
- **C:** all streaming output modes, stateful alerts, late-data measurement, the exactly-once kill-the-driver test, optional streaming ML scoring.
- **D:** gold tables (window functions, pivots, custom aggregations), broadcast and sort-merge joins, bucketing, caching and CBO experiments with numbers.
- **E:** Streamlit app, alerting rules, Loki logs, merging batch and realtime views, and assembling the final report.
- **Shared:** MLlib (C and D) and GraphFrames (D and E), plus Lessons Learned: each member writes the lessons for their own categories from their journal.
