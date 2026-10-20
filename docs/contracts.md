# Contracts

> **Rule: changes need a PR approved by the owners of every affected layer.**
> A contract change updates this file, the code that defines it (`src/cryptopipe/contracts.py`,
> `src/cryptopipe/schemas/`, `conf/symbols.yaml`, `cql/schema.cql`) and its tests in the same PR.
> Frozen for Checkpoint 1 after the team review.

These are the interfaces between the layers. Code never hardcodes a topic name, lake path or table name:
it imports the constant from `cryptopipe.contracts` (`crypto contracts show` prints them all).

| Contract | Source of truth | Producers | Consumers |
|---|---|---|---|
| Symbols | `conf/symbols.yaml`, `cryptopipe.symbols.SymbolMap` | everyone | everyone |
| Message schemas | `cryptopipe.schemas` (pydantic v2) | B, C, D | B, C, D, E |
| Kafka topics | `cryptopipe.contracts.TOPICS` | B, C, D | C, D, E |
| Lake layout | `cryptopipe.contracts.LAKE_*` | C, D | C, D, E |
| Cassandra tables | `cql/schema.cql`, `cryptopipe.contracts.CASSANDRA_*` | C, D | E |
| Env vars | `cryptopipe.config.Settings`, `.env.example` | A (deploy) | everyone |

## 1. Conventions

* **Event time** is the exchange timestamp `event_ts`, **UTC epoch milliseconds**. **Ingestion time** is
  `ingest_ts` (when the producer received the message). Never mix them. Timestamps in other units (the Binance
  archive uses microseconds since 2025) are converted to ms at the boundary.
* **Symbols** are canonical `BASE-USDT` (`BTC-USDT`). Exchange names map via `SymbolMap`:
  `to_canonical("binance", "BTCUSDT")`, `to_exchange("coinbase", "BTC-USDT") -> "BTC-USD"`.
  Coinbase `-USD` pairs map to the `-USDT` canonical symbol (documented approximation).
* **Dedupe key** for trades: `(exchange, symbol, trade_id)` (`contracts.TRADE_KEY`).
* Kafka values are UTF-8 JSON produced by `model.to_json()`; every message has `schema_version` (currently `1`).
  Unknown fields are rejected (`extra="forbid"`). Avro + Schema Registry is planned as version 2 (B1).
* Spark jobs read JSON with `to_spark_schema(Model)`, never a hand-written schema.

## 2. Symbols

| Canonical | Binance | Coinbase | CoinGecko id |
|---|---|---|---|
| BTC-USDT | BTCUSDT | BTC-USD | bitcoin |
| ETH-USDT | ETHUSDT | ETH-USD | ethereum |
| BNB-USDT | BNBUSDT | (not listed) | binancecoin |
| SOL-USDT | SOLUSDT | SOL-USD | solana |
| XRP-USDT | XRPUSDT | XRP-USD | ripple |
| ADA-USDT | ADAUSDT | ADA-USD | cardano |
| DOGE-USDT | DOGEUSDT | DOGE-USD | dogecoin |
| AVAX-USDT | AVAXUSDT | AVAX-USD | avalanche-2 |
| LINK-USDT | LINKUSDT | LINK-USD | chainlink |
| LTC-USDT | LTCUSDT | LTC-USD | litecoin |

## 3. Kafka topics

| Topic | Key | Partitions | Retention | Value model | Producer | Consumers |
|---|---|---|---|---|---|---|
| `raw.trades` | canonical symbol | 12 | 7 days | `Trade` | B (WS producers), D (replayer) | C |
| `raw.klines` | canonical symbol | 6 | 7 days | `Kline` (closed candles only) | B | C |
| `ref.asset_meta` | canonical symbol | 1 | compacted | `AssetMeta` | B | C, D |
| `ref.fear_greed` | UTC day `yyyy-MM-dd` | 1 | compacted | `FearGreed` | B | D |
| `alerts` | canonical symbol | 3 | 7 days | `Alert` | C | E |
| `dlq.ingest` | source (e.g. `binance_ws`) | 3 | 7 days | `DlqRecord` | B, D | E |

Keying by canonical symbol keeps per-symbol ordering inside one partition.

### `raw.trades`: `Trade`

| Field | Type | Notes |
|---|---|---|
| `exchange` | `"binance"` \| `"coinbase"` | |
| `symbol` | string | canonical, must be in `conf/symbols.yaml` |
| `trade_id` | string | exchange trade id as a string |
| `price` | double | > 0 |
| `qty` | double | > 0, base asset |
| `side` | `"buy"` \| `"sell"` | taker side (Binance: `m=true` means the buyer is the maker, so side = `sell`) |
| `event_ts` | long | exchange time, UTC ms |
| `ingest_ts` | long | producer receive time, UTC ms |
| `schema_version` | long | `1` |

```json
{"exchange":"binance","symbol":"BTC-USDT","trade_id":"5205379727","price":62000.5,"qty":0.0125,"side":"buy","event_ts":1760000000000,"ingest_ts":1760000000087,"schema_version":1}
```

### `raw.klines`: `Kline`

Fields: `exchange`, `symbol`, `interval` (`"1m"`), `open_ts`, `close_ts` (ms), `open`, `high`, `low`, `close`,
`volume` (base), `quote_volume`, `trades` (count), `is_closed`, `schema_version`. Validation: `close_ts >= open_ts`,
`low <= min(open, close)`, `high >= max(open, close)`.

```json
{"exchange":"binance","symbol":"ETH-USDT","interval":"1m","open_ts":1760000040000,"close_ts":1760000099999,"open":4510.2,"high":4512.8,"low":4508.9,"close":4511.6,"volume":152.34,"quote_volume":687289.5,"trades":1843,"is_closed":true,"schema_version":1}
```

### `ref.asset_meta`: `AssetMeta`

Fields: `coingecko_id`, `symbol` (canonical), `name`, `categories` (list of strings), `market_cap` (USD, nullable),
`circulating_supply` (nullable), `snapshot_ts` (ms), `schema_version`.

```json
{"coingecko_id":"bitcoin","symbol":"BTC-USDT","name":"Bitcoin","categories":[],"market_cap":1230000000000.0,"circulating_supply":19930000.0,"snapshot_ts":1760000000000,"schema_version":1}
```

### `ref.fear_greed`: `FearGreed`

Fields: `value` (0..100), `classification`, `ts` (start of the UTC day, ms), `schema_version`.

```json
{"value":42,"classification":"Fear","ts":1759968000000,"schema_version":1}
```

### `alerts`: `Alert`

Fields: `alert_id` (deterministic, so replays upsert), `symbol`, `type`, `severity`
(`info` \| `warning` \| `critical`), `value`, `threshold`, `event_ts` (ms), `details` (map string → string),
`schema_version`.

```json
{"alert_id":"price_spike:SOL-USDT:1760000040000","symbol":"SOL-USDT","type":"price_spike","severity":"warning","value":0.062,"threshold":0.05,"event_ts":1760000040000,"details":{"window":"1m","direction":"up"},"schema_version":1}
```

### `dlq.ingest`: `DlqRecord`

Fields: `source`, `reason`, `raw` (original payload, unchanged), `ingest_ts` (ms), `schema_version`.

```json
{"source":"binance_ws","reason":"unknown symbol FOOUSDT","raw":"{\"e\":\"trade\",\"s\":\"FOOUSDT\"}","ingest_ts":1760000000000,"schema_version":1}
```

## 4. Lake layout

Root: `CRYPTO_LAKE_ROOT` (local `file:///tmp/crypto-lake`, cluster `hdfs://hdfs-namenode.storage:8020/lake`).
Partition columns: `date` (string `yyyy-MM-dd`, UTC from event time) and `hour` (int 0..23). Format: Parquet (zstd).

| Constant | Path | Written by | Partitions | Content |
|---|---|---|---|---|
| `LAKE_BRONZE_TRADES` | `bronze/trades` | `stream_bronze` (C) | date, hour | raw Kafka records of `raw.trades` |
| `LAKE_BRONZE_KLINES` | `bronze/klines` | `stream_bronze` (C) | date, hour | raw Kafka records of `raw.klines` |
| `LAKE_BRONZE_ARCHIVE_TRADES` | `bronze/archive_trades` | `batch_load_archive` (D) | date | archive trades, same columns as bronze |
| `LAKE_BRONZE_ARCHIVE_KLINES` | `bronze/archive_klines` | `batch_load_archive` (D) | date | archive klines |
| `LAKE_SILVER_TRADES` | `silver/trades` | `batch_silver` (D) | date | typed `Trade` columns + `event_time`, validated, deduped |
| `LAKE_SILVER_KLINES` | `silver/klines` | `batch_silver` (D) | date | typed `Kline` columns |
| `LAKE_GOLD_OHLCV_1M` | `gold/ohlcv_1m` | demo, `stream_ohlcv` (C) | date | `OHLCV_1M_SCHEMA` |
| `LAKE_GOLD` | `gold/<table>` | D (B2) | per table | indicators, daily stats, ... |
| `LAKE_QUARANTINE_TRADES` | `quarantine/trades` | C, D | date | rejected rows + `reason` |
| `LAKE_QUARANTINE_KLINES` | `quarantine/klines` | C, D | date | rejected rows + `reason` |
| `LAKE_CHECKPOINTS` | `_checkpoints/<job>/<query>` | streaming jobs (C) | | `CRYPTO_CHECKPOINT_ROOT` overrides |
| `LAKE_PROFILE` | `_profile` | `profile` job (D) | | data profile reports |

**Bronze columns** (stream and archive, `contracts.BRONZE_COLUMNS`):
`key` (string), `value` (string, raw JSON), `topic`, `partition` (int), `offset` (long), `kafka_ts` (timestamp),
`source` (`kafka` or `archive`), `date`, `hour`. Archive rows use `topic = "archive"`, `partition = -1`,
`offset` = row number, and `value` = the row converted to `Trade`/`Kline` JSON, so silver parses both the same way.
A streaming sink owns its directory (`_spark_metadata`), so batch loads never write into `bronze/trades`.

**OHLCV 1m columns** (`contracts.OHLCV_1M_SCHEMA`): `exchange`, `symbol`, `window_start`, `window_end`
(timestamps, UTC), `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trades` (long), `vwap`, `date`.
`open`/`close` are the prices of the first/last trade by `event_ts` (ties broken by `trade_id`), so out-of-order
arrival does not change a candle. `vwap = quote_volume / volume`.

## 5. Cassandra (keyspace `crypto`)

Schema: [`cql/schema.cql`](../cql/schema.cql). Owner: E.

| Table | Primary key | Written by | Query it serves |
|---|---|---|---|
| `ohlcv_1m` | `((symbol, day), window_start DESC, exchange)`, TTL 30 d | `stream_ohlcv` (C) | candles for one symbol over a time range |
| `latest_price` | `(symbol, exchange)`, write timestamp = event time | `stream_ohlcv` (C) | latest price board |
| `alerts` | `((symbol, day), event_ts DESC, alert_id)`, TTL 90 d | C (B3) | recent alerts per symbol |
| `daily_stats` | `(symbol, day DESC)` | D (B2) | daily batch view per symbol |

## 6. Environment variables

All prefixed `CRYPTO_`, read by `cryptopipe.config.Settings`. Every variable has a local default.

| Env var | Default (local) | Cluster value | Used by |
|---|---|---|---|
| `CRYPTO_KAFKA_BOOTSTRAP` | `localhost:9092` | `crypto-kafka-bootstrap.kafka:9092` | B, C |
| `CRYPTO_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT` (B5: `SASL_SSL`) | B, C |
| `CRYPTO_LAKE_ROOT` | `file:///tmp/crypto-lake` | `hdfs://hdfs-namenode.storage:8020/lake` | C, D |
| `CRYPTO_CHECKPOINT_ROOT` | `${LAKE_ROOT}/_checkpoints` | same | C |
| `CRYPTO_CASSANDRA_HOSTS` | `localhost` | `cassandra.storage` | C, D, E |
| `CRYPTO_CASSANDRA_KEYSPACE` | `crypto` | `crypto` | C, D, E |
| `CRYPTO_SYMBOLS_FILE` | `conf/symbols.yaml` | `/opt/cryptopipe/conf/symbols.yaml` (in images) | all |
| `CRYPTO_LOG_LEVEL` | `INFO` | `INFO` | all |
| `CRYPTO_METRICS_PORT` | `8000` | `8000` | B |
