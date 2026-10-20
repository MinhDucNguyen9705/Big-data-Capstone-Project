# Owner: C (Speed layer) | Task: A5 | Status: batch implemented by the bootstrap; streaming is C's task
"""Trade deduplication on the business key `(exchange, symbol, trade_id)`."""

from __future__ import annotations

from pyspark.sql import DataFrame

from cryptopipe.contracts import TRADE_KEY


def dedupe_trades(df: DataFrame, streaming: bool = False, watermark: str = "2 minutes") -> DataFrame:
    """Drop repeated trades (producer retries, reconnect backfill, Coinbase snapshots, stream/archive overlap).

    Batch: `dropDuplicates(TRADE_KEY)`.
    Streaming: `withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(TRADE_KEY)` (state bounded
    by the watermark).
    """
    if streaming:
        raise NotImplementedError("Task A5 – owner C: streaming dedupe with dropDuplicatesWithinWatermark")
    return df.dropDuplicates(TRADE_KEY)
