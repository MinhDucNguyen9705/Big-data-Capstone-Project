# Owner: D (Batch and data) | Task: A6 (batch_silver, klines) | Status: implemented
"""Kline bronze -> silver: parse `Kline` JSON, validate, dedupe. Same shape as the trade transforms."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from cryptopipe.contracts import QUARANTINE_REASON
from cryptopipe.schemas import Kline, to_spark_schema
from cryptopipe.schemas.common import EXCHANGES
from cryptopipe.symbols import load_symbols
from cryptopipe.transforms.normalize import RAW_COLUMN, add_time_columns

KLINE_SCHEMA = to_spark_schema(Kline)
KLINE_COLUMNS = KLINE_SCHEMA.fieldNames()
# Dedupe business key for candles: one row per exchange, symbol, interval and open time.
KLINE_KEY = ["exchange", "symbol", "interval", "open_ts"]
_PRICES = ["open", "high", "low", "close"]


def parse_klines(df_kafka: DataFrame) -> DataFrame:
    """Kafka-shaped DataFrame (`value` = Kline JSON) -> `Kline` columns + `event_time`/`date`/`hour` (from
    `open_ts`) + `raw`. Malformed JSON yields null fields, rejected by `validate_klines` as `malformed`."""
    raw = F.col("value").cast("string")
    parsed = df_kafka.select(raw.alias(RAW_COLUMN), F.from_json(raw, KLINE_SCHEMA).alias("k"))
    klines = parsed.select(*(F.col(f"k.{c}").alias(c) for c in KLINE_COLUMNS), RAW_COLUMN)
    return add_time_columns(klines, "open_ts")


def kline_rules(symbols: list[str]) -> list[tuple[str, Column]]:
    """Ordered (reason, condition-that-fails) pairs; the first failing rule is the row's reason."""
    return [
        ("malformed", F.greatest(*(F.col(c).isNull() for c in KLINE_COLUMNS))),
        ("unknown_exchange", ~F.col("exchange").isin(*EXCHANGES)),
        ("unknown_symbol", ~F.col("symbol").isin(*symbols)),
        ("non_positive_price", F.least(*(F.col(c) for c in _PRICES)) <= 0),
        ("negative_volume", (F.col("volume") < 0) | (F.col("quote_volume") < 0) | (F.col("trades") < 0)),
        ("close_before_open", F.col("close_ts") < F.col("open_ts")),
        (
            "inconsistent_ohlc",
            (F.col("low") > F.least("open", "close")) | (F.col("high") < F.greatest("open", "close")),
        ),
        ("not_closed", ~F.col("is_closed")),
    ]


def validate_klines(df: DataFrame, symbols: list[str] | None = None) -> tuple[DataFrame, DataFrame]:
    """Split parsed klines into (good, bad) with the `Kline` model's rules plus closed candles only.

    `good` drops the `raw` column; `bad` keeps every column plus `reason`. `symbols` defaults to
    `conf/symbols.yaml`.
    """
    symbols = symbols if symbols is not None else load_symbols().canonicals
    reason = F.lit(None).cast("string")
    for name, failed in reversed(kline_rules(symbols)):
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    flagged = df.withColumn(QUARANTINE_REASON, reason)
    good = flagged.filter(F.col(QUARANTINE_REASON).isNull()).drop(QUARANTINE_REASON, RAW_COLUMN)
    bad = flagged.filter(F.col(QUARANTINE_REASON).isNotNull())
    return good, bad


def dedupe_klines(df: DataFrame) -> DataFrame:
    """One candle per `KLINE_KEY` (stream and archive both carry closed Binance candles)."""
    return df.dropDuplicates(KLINE_KEY)
