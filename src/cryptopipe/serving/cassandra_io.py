# Owners: E (serving), C (A6 streaming writers)
"""Cassandra driver sessions and distributed foreachBatch sinks for the speed layer."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.config import Settings

if TYPE_CHECKING:
    from cassandra.cluster import Session


def get_session(settings: Settings) -> Session:
    """Driver session; callers own both session.shutdown() and session.cluster.shutdown()."""
    from cassandra.cluster import Cluster

    cluster = Cluster(settings.cassandra_host_list)
    try:
        return cluster.connect(settings.cassandra_keyspace)
    except Exception:
        cluster.shutdown()
        raise


def ohlcv_to_rows(df: DataFrame) -> DataFrame:
    """Project only CQL columns; Spark writes timestamps as UTC instants."""
    return (
        df.withColumn("day", F.to_date("window_start"))
        .withColumn("updated_at", F.current_timestamp())
        .drop(contracts.PARTITION_DATE)
    )


def latest_price_to_rows(df: DataFrame) -> DataFrame:
    """Newest trade per key in this micro-batch; writetime prevents older batches winning."""
    newest = df.groupBy("symbol", "exchange").agg(
        F.max_by(F.struct("price", "qty", "event_ts"), F.struct("event_ts", "trade_id")).alias("last")
    )
    return newest.select(
        "symbol",
        "exchange",
        "last.price",
        "last.qty",
        F.timestamp_millis("last.event_ts").alias("event_ts"),
        (F.col("last.event_ts") * F.lit(1000)).cast("long").alias("write_ts"),
    )


def _write(df: DataFrame, table: str, settings: Settings, **options: str) -> None:
    # No collect()/Python per-row network writes: the connector distributes work to executors.
    # Do not set a default TTL here; the existing table schema owns retention.
    df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table=table,
        keyspace=settings.cassandra_keyspace,
        **{"spark.cassandra.connection.host": settings.cassandra_hosts, **options},
    ).save()


def write_ohlcv(df_batch: DataFrame, batch_id: int, *, settings: Settings | None = None) -> None:
    """Update-mode cumulative candles upsert by ((symbol, day), window_start, exchange).

    One active query/checkpoint must own the table's keys. A retried batch rewrites its values; this is an
    at-least-once connector sink, not a cross-sink transaction (TTL/updated_at can refresh on retries).
    """
    _write(ohlcv_to_rows(df_batch), contracts.CASSANDRA_OHLCV_1M, settings or Settings())


def write_latest_price(df_batch: DataFrame, batch_id: int, *, settings: Settings | None = None) -> None:
    """Use exchange event time in microseconds as Cassandra's per-row write timestamp."""
    _write(
        latest_price_to_rows(df_batch),
        contracts.CASSANDRA_LATEST_PRICE,
        settings or Settings(),
        writetime="write_ts",
    )
