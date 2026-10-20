# Owner: C (Speed layer) | Task: A5 | Status: minimal implementation from the bootstrap; C hardens it
"""Kafka record -> typed trade columns, using the shared `Trade` contract schema."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cryptopipe.contracts import PARTITION_DATE, PARTITION_HOUR
from cryptopipe.schemas import Trade, to_spark_schema

TRADE_SCHEMA = to_spark_schema(Trade)
TRADE_COLUMNS = TRADE_SCHEMA.fieldNames()
RAW_COLUMN = "raw"  # original JSON, kept so rejected rows can be quarantined unchanged


def add_time_columns(df: DataFrame, ts_col: str = "event_ts") -> DataFrame:
    """Add `event_time` (timestamp, from epoch ms), `date` (yyyy-MM-dd) and `hour`, all UTC."""
    return (
        df.withColumn("event_time", F.timestamp_millis(F.col(ts_col)))
        .withColumn(PARTITION_DATE, F.date_format("event_time", "yyyy-MM-dd"))
        .withColumn(PARTITION_HOUR, F.hour("event_time"))
    )


def parse_trades(df_kafka: DataFrame) -> DataFrame:
    """Kafka-shaped DataFrame (a `value` column, string or binary JSON) -> typed trades.

    Output: `Trade` columns + `event_time`, `date`, `hour` + `raw`. Malformed JSON yields a row with null
    fields (and the original `raw`), which `quality.validate_trades` rejects as `malformed`.
    Works on batch and streaming DataFrames.
    """
    raw = F.col("value").cast("string")
    parsed = df_kafka.select(raw.alias(RAW_COLUMN), F.from_json(raw, TRADE_SCHEMA).alias("t"))
    trades = parsed.select(*(F.col(f"t.{c}").alias(c) for c in TRADE_COLUMNS), RAW_COLUMN)
    return add_time_columns(trades)
