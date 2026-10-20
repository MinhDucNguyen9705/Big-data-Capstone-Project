# Owner: C (Speed layer) | Task: A5 | Status: minimal implementation from the bootstrap; C hardens it
"""Data quality rules for trades. Rejected rows are never dropped silently: they carry a `reason`."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from cryptopipe.contracts import QUARANTINE_REASON
from cryptopipe.schemas.common import EXCHANGES
from cryptopipe.symbols import load_symbols
from cryptopipe.transforms.normalize import RAW_COLUMN

MAX_FUTURE_MS = 5 * 60 * 1000
REQUIRED = ["exchange", "symbol", "trade_id", "price", "qty", "side", "event_ts", "ingest_ts"]


def trade_rules(symbols: list[str]) -> list[tuple[str, Column]]:
    """Ordered (reason, condition-that-fails) pairs; the first failing rule is the row's reason."""
    now_ms = F.expr("unix_millis(current_timestamp())")
    return [
        ("malformed", F.greatest(*(F.col(c).isNull() for c in REQUIRED))),
        ("unknown_exchange", ~F.col("exchange").isin(*EXCHANGES)),
        ("unknown_symbol", ~F.col("symbol").isin(*symbols)),
        ("non_positive_price", F.col("price") <= 0),
        ("non_positive_qty", F.col("qty") <= 0),
        ("invalid_side", ~F.col("side").isin("buy", "sell")),
        (
            "future_timestamp",
            (F.col("event_ts") > F.col("ingest_ts") + MAX_FUTURE_MS) | (F.col("event_ts") > now_ms + MAX_FUTURE_MS),
        ),
    ]


def validate_trades(df: DataFrame, symbols: list[str] | None = None) -> tuple[DataFrame, DataFrame]:
    """Split parsed trades into (good, bad).

    Rules: required fields present, known exchange and symbol, price > 0, qty > 0, side buy/sell, `event_ts` at
    most 5 min after `ingest_ts` and after now. `good` drops the `raw` column; `bad` keeps every column plus
    `reason`. `symbols` defaults to the canonical symbols of `conf/symbols.yaml`.
    """
    symbols = symbols if symbols is not None else load_symbols().canonicals
    reason = F.lit(None).cast("string")
    for name, failed in reversed(trade_rules(symbols)):
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    flagged = df.withColumn(QUARANTINE_REASON, reason)
    good = flagged.filter(F.col(QUARANTINE_REASON).isNull()).drop(QUARANTINE_REASON, RAW_COLUMN)
    bad = flagged.filter(F.col(QUARANTINE_REASON).isNotNull())
    return good, bad
