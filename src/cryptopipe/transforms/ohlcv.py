# Owner: C (Speed layer) | Task: A5 | Status: minimal implementation from the bootstrap; C hardens it
"""1-minute OHLCV candles from validated, deduplicated trades."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cryptopipe.contracts import OHLCV_1M_SCHEMA, PARTITION_DATE


def ohlcv_1m(df: DataFrame) -> DataFrame:
    """Group trades by `window(event_time, "1 minute")`, exchange and symbol.

    open/close = price of the first/last trade by `(event_ts, trade_id)`, so out-of-order arrival does not change
    the candle; volume = sum(qty), quote_volume = sum(price * qty), vwap = quote_volume / volume.
    Output columns and order: `contracts.OHLCV_1M_SCHEMA`. Works on batch and (watermarked) streaming input.
    """
    order = F.struct("event_ts", "trade_id")
    candles = df.groupBy(F.window("event_time", "1 minute").alias("w"), "exchange", "symbol").agg(
        F.min_by("price", order).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order).alias("close"),
        F.sum("qty").alias("volume"),
        F.sum(F.col("price") * F.col("qty")).alias("quote_volume"),
        F.count(F.lit(1)).alias("trades"),
    )
    return candles.select(
        "exchange",
        "symbol",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades",
        (F.col("quote_volume") / F.col("volume")).alias("vwap"),
        F.date_format("w.start", "yyyy-MM-dd").alias(PARTITION_DATE),
    ).select(*OHLCV_1M_SCHEMA.fieldNames())
