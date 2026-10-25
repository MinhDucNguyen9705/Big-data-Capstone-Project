"""Frozen interfaces between layers: Kafka topics, lake layout, Cassandra tables.

Import these constants instead of writing topic names, paths or table names as strings.
CONTRACT: changes need a PR that updates this module, its tests and docs/architecture.md (section 6), approved
by the owners of every affected layer.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cryptopipe.schemas import Alert, AssetMeta, DlqRecord, FearGreed, Kline, Trade

# ---------------------------------------------------------------------------------------------------------
# Kafka topics

DAY_MS = 24 * 3600 * 1000


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    key: str  # what the Kafka message key is
    value_model: type
    compacted: bool = False
    retention_ms: int | None = 7 * DAY_MS  # None = infinite (compacted topics)
    producer: str = ""  # owning member
    consumers: str = ""


TOPIC_RAW_TRADES = Topic(
    "raw.trades", 12, "canonical symbol", Trade, producer="B", consumers="C (stream_bronze, stream_ohlcv)"
)
TOPIC_RAW_KLINES = Topic("raw.klines", 6, "canonical symbol", Kline, producer="B", consumers="C (stream_bronze)")
TOPIC_ASSET_META = Topic(
    "ref.asset_meta", 1, "canonical symbol", AssetMeta, compacted=True, retention_ms=None,
    producer="B", consumers="C, D (joins)",
)  # fmt: skip
TOPIC_FEAR_GREED = Topic(
    "ref.fear_greed", 1, "UTC day yyyy-MM-dd", FearGreed, compacted=True, retention_ms=None,
    producer="B", consumers="D (joins)",
)  # fmt: skip
TOPIC_ALERTS = Topic("alerts", 3, "canonical symbol", Alert, producer="C", consumers="E")
TOPIC_DLQ = Topic("dlq.ingest", 3, "source", DlqRecord, producer="B, D", consumers="E (monitoring)")

TOPICS: list[Topic] = [
    TOPIC_RAW_TRADES,
    TOPIC_RAW_KLINES,
    TOPIC_ASSET_META,
    TOPIC_FEAR_GREED,
    TOPIC_ALERTS,
    TOPIC_DLQ,
]

# ---------------------------------------------------------------------------------------------------------
# Lake layout, relative to settings.lake_root (use settings.lake_path(...)).

PARTITION_DATE = "date"  # string yyyy-MM-dd (UTC, from event time)
PARTITION_HOUR = "hour"  # int 0..23 (UTC)

LAKE_BRONZE_TRADES = "bronze/trades"  # stream_bronze sink for raw.trades; partitioned by date, hour
LAKE_BRONZE_KLINES = "bronze/klines"  # stream_bronze sink for raw.klines; partitioned by date, hour
LAKE_BRONZE_ARCHIVE_TRADES = "bronze/archive_trades"  # batch_load_archive; partitioned by date
LAKE_BRONZE_ARCHIVE_KLINES = "bronze/archive_klines"  # batch_load_archive; partitioned by date
LAKE_SILVER_TRADES = "silver/trades"  # batch_silver; typed, validated, deduped; partitioned by date
LAKE_SILVER_KLINES = "silver/klines"  # batch_silver; partitioned by date
LAKE_GOLD = "gold"  # gold/<table>, defined in Part B (B2)
LAKE_GOLD_OHLCV_1M = "gold/ohlcv_1m"  # 1m candles (demo, stream_ohlcv append sink); partitioned by date
LAKE_QUARANTINE_TRADES = "quarantine/trades"  # stream_ohlcv (C) rejects, streaming file sink; partitioned by date
LAKE_QUARANTINE_ARCHIVE_TRADES = "quarantine/archive_trades"  # batch_silver (D) rejects of archive trades; by date
LAKE_QUARANTINE_KLINES = "quarantine/klines"  # batch_silver (D) kline rejects; partitioned by date
LAKE_RAW_ARCHIVE = "raw/archive"  # landing zone: downloaded archive CSVs, <kind>/<SYMBOL>/*.csv (D)
LAKE_CHECKPOINTS = "_checkpoints"  # <job>/<query>; settings.checkpoint_root overrides
LAKE_PROFILE = "_profile"  # data profile reports (D, task A8)

LAKE_PATHS: dict[str, str] = {
    "bronze_trades": LAKE_BRONZE_TRADES,
    "bronze_klines": LAKE_BRONZE_KLINES,
    "bronze_archive_trades": LAKE_BRONZE_ARCHIVE_TRADES,
    "bronze_archive_klines": LAKE_BRONZE_ARCHIVE_KLINES,
    "silver_trades": LAKE_SILVER_TRADES,
    "silver_klines": LAKE_SILVER_KLINES,
    "gold": LAKE_GOLD,
    "gold_ohlcv_1m": LAKE_GOLD_OHLCV_1M,
    "quarantine_trades": LAKE_QUARANTINE_TRADES,
    "quarantine_archive_trades": LAKE_QUARANTINE_ARCHIVE_TRADES,
    "quarantine_klines": LAKE_QUARANTINE_KLINES,
    "raw_archive": LAKE_RAW_ARCHIVE,
    "checkpoints": LAKE_CHECKPOINTS,
    "profile": LAKE_PROFILE,
}

# Columns of every bronze table (stream and archive): the raw record plus lineage.
# For archive rows: topic = "archive", partition = -1, offset = row number in the file.
BRONZE_COLUMNS = ["key", "value", "topic", "partition", "offset", "kafka_ts", "source", "date", "hour"]

# Extra column on quarantine rows: why the row was rejected.
QUARANTINE_REASON = "reason"

# Dedupe business key for trades.
TRADE_KEY = ["exchange", "symbol", "trade_id"]

# Output of transforms.ohlcv.ohlcv_1m and the gold/ohlcv_1m table (also feeds Cassandra ohlcv_1m).
OHLCV_1M_SCHEMA = StructType(
    [
        StructField("exchange", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("quote_volume", DoubleType(), False),
        StructField("trades", LongType(), False),
        StructField("vwap", DoubleType(), False),
        StructField("date", StringType(), False),
    ]
)

# ---------------------------------------------------------------------------------------------------------
# Cassandra (keyspace = settings.cassandra_keyspace, schema in cql/schema.cql)

CASSANDRA_OHLCV_1M = "ohlcv_1m"  # PK ((symbol, day), window_start, exchange); speed layer (C)
CASSANDRA_LATEST_PRICE = "latest_price"  # PK (symbol, exchange); speed layer (C)
CASSANDRA_ALERTS = "alerts"  # PK ((symbol, day), event_ts, alert_id); speed layer (C, B3)
CASSANDRA_DAILY_STATS = "daily_stats"  # PK (symbol, day); batch views (D, B2)

CASSANDRA_TABLES: dict[str, str] = {
    CASSANDRA_OHLCV_1M: "((symbol, day), window_start DESC, exchange)",
    CASSANDRA_LATEST_PRICE: "((symbol), exchange)",
    CASSANDRA_ALERTS: "((symbol, day), event_ts DESC, alert_id)",
    CASSANDRA_DAILY_STATS: "((symbol), day DESC)",
}
