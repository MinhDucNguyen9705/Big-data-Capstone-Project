# Owner: C (Speed layer) | Task: A5
"""Data quality rules for trades. Rejected rows are never dropped silently: they carry a `reason`."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from cryptopipe.contracts import QUARANTINE_REASON
from cryptopipe.schemas import Trade
from cryptopipe.schemas.common import EXCHANGES, SCHEMA_VERSION
from cryptopipe.symbols import load_symbols
from cryptopipe.transforms.normalize import RAW_COLUMN, TRADE_COLUMNS

MAX_FUTURE_MS = 5 * 60 * 1000
REQUIRED = TRADE_COLUMNS
_EPOCH = Trade.model_json_schema()["properties"]["event_ts"]


def trade_rules(symbols: list[str]) -> list[tuple[str, Column]]:
    """Ordered (reason, condition-that-fails) pairs; the first failing rule is the row's reason."""
    now_ms = F.expr("unix_millis(current_timestamp())")
    return [
        ("malformed", F.greatest(*(F.col(c).isNull() for c in REQUIRED))),
        (
            "unknown_fields",
            F.size(F.array_except(F.json_object_keys(RAW_COLUMN), F.array(*(F.lit(c) for c in TRADE_COLUMNS)))) > 0,
        ),
        ("unsupported_schema_version", F.col("schema_version") != SCHEMA_VERSION),
        ("unknown_exchange", ~F.col("exchange").isin(*EXCHANGES)),
        ("unknown_symbol", ~F.col("symbol").isin(*symbols)),
        ("empty_trade_id", F.length(F.trim("trade_id")) == 0),
        ("non_finite_price", F.isnan("price") | (F.abs("price") == float("inf"))),
        ("non_finite_qty", F.isnan("qty") | (F.abs("qty") == float("inf"))),
        ("non_positive_price", F.col("price") <= 0),
        ("non_positive_qty", F.col("qty") <= 0),
        ("invalid_side", ~F.col("side").isin("buy", "sell")),
        (
            "invalid_timestamp",
            (F.col("event_ts") < _EPOCH["minimum"])
            | (F.col("event_ts") >= _EPOCH["exclusiveMaximum"])
            | (F.col("ingest_ts") < _EPOCH["minimum"])
            | (F.col("ingest_ts") >= _EPOCH["exclusiveMaximum"]),
        ),
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
