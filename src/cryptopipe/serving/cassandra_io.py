# Owner: E (Serving and docs) | Task: A6 (Cassandra sink) / A9 | Status: stub, signatures frozen by the bootstrap
"""Cassandra access: a driver session for readers and `foreachBatch` writers for the speed layer."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from cryptopipe.config import Settings

if TYPE_CHECKING:
    from cassandra.cluster import Session


def get_session(settings: Settings) -> Session:
    """Driver session on `settings.cassandra_host_list`, keyspace `settings.cassandra_keyspace`."""
    raise NotImplementedError("Task A6 – owner E")


def ohlcv_to_rows(df: DataFrame) -> DataFrame:
    """`OHLCV_1M_SCHEMA` -> columns of table `ohlcv_1m` (adds `day` = date(window_start), `updated_at`)."""
    raise NotImplementedError("Task A6 – owner E")


def write_ohlcv(df_batch: DataFrame, batch_id: int) -> None:
    """`foreachBatch` sink: upsert candles into `ohlcv_1m` with the Spark Cassandra connector.

    Idempotent by primary key, so a replayed micro-batch rewrites the same rows.
    """
    raise NotImplementedError("Task A6 – owner E")


def write_latest_price(df_batch: DataFrame, batch_id: int) -> None:
    """`foreachBatch` sink: upsert `latest_price` with write timestamp = event time (older never wins)."""
    raise NotImplementedError("Task A6 – owner E")
