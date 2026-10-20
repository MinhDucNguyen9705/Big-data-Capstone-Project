# Owner: D (Batch and data) | Task: A6 (batch_load_archive) | Status: stub, signatures frozen by the bootstrap
"""Binance archive CSV -> contract columns, so archive history and live stream share one silver schema."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, DoubleType, LongType, StructField, StructType

# Headerless trade archive CSV columns
ARCHIVE_TRADES_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("price", DoubleType()),
        StructField("qty", DoubleType()),
        StructField("quote_qty", DoubleType()),
        StructField("time", LongType()),  # ms before 2025, microseconds from 2025-01-01
        StructField("is_buyer_maker", BooleanType()),
        StructField("is_best_match", BooleanType()),
    ]
)


def epoch_to_ms(col_name: str):
    """Column expression normalizing an epoch in s/ms/us/ns to ms, detected by magnitude."""
    raise NotImplementedError("Task A6 – owner D")


def archive_trades_to_contract(df_csv: DataFrame, symbol: str) -> DataFrame:
    """Archive trades (ARCHIVE_TRADES_SCHEMA) for canonical `symbol` -> `Trade` columns.

    exchange = "binance", trade_id = string(id), side = "sell" if is_buyer_maker else "buy",
    event_ts = time normalized us -> ms, ingest_ts = load time, schema_version = 1.
    """
    raise NotImplementedError("Task A6 – owner D")
