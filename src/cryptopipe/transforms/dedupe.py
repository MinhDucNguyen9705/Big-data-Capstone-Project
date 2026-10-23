# Owner: C (Speed layer) | Task: A5
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
        if not df.isStreaming:
            raise ValueError("streaming=True requires a streaming DataFrame")
        return df.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(TRADE_KEY)
    return df.dropDuplicates(TRADE_KEY)
