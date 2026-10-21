# Owner: C (Speed layer) | Task: A5
"""Kafka record -> typed trade columns, using the shared `Trade` contract schema."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from cryptopipe.contracts import PARTITION_DATE, PARTITION_HOUR
from cryptopipe.schemas import Trade, to_spark_schema

TRADE_SCHEMA = to_spark_schema(Trade)
TRADE_COLUMNS = TRADE_SCHEMA.fieldNames()
RAW_COLUMN = "raw"  # original JSON, kept so rejected rows can be quarantined unchanged
_PARSE_SCHEMA = StructType([*TRADE_SCHEMA.fields, StructField("_corrupt_record", StringType())])
_EPOCH = Trade.model_json_schema()["properties"]["event_ts"]


def add_time_columns(df: DataFrame, ts_col: str = "event_ts") -> DataFrame:
    """Add `event_time` (timestamp, from epoch ms), `date` (yyyy-MM-dd) and `hour`, all UTC."""
    return (
        df.withColumn(
            "event_time",
            F.when(
                (F.col(ts_col) >= _EPOCH["minimum"]) & (F.col(ts_col) < _EPOCH["exclusiveMaximum"]),
                F.timestamp_millis(F.col(ts_col)),
            ),
        )
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
    # Preserve lineage when supplied; value-only batch fixtures keep their original output schema.
    metadata = [c for c in ("topic", "partition", "offset", "timestamp", "kafka_ts") if c in df_kafka.columns]
    parsed = df_kafka.select(raw.alias(RAW_COLUMN), F.from_json(raw, _PARSE_SCHEMA).alias("t"), *metadata)
    # Spark's permissive parser can return partial fields for broken JSON. Reject the whole record.
    trades = parsed.select(
        *(F.when(F.col("t._corrupt_record").isNull(), F.col(f"t.{c}")).alias(c) for c in TRADE_COLUMNS),
        RAW_COLUMN,
        *metadata,
    )
    return add_time_columns(trades)
