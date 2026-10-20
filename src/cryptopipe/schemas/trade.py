"""`Trade`: value of topic `raw.trades` (CONTRACT, see docs/contracts.md)."""

from __future__ import annotations

from typing import Literal

from cryptopipe.schemas.common import (
    SCHEMA_VERSION,
    CanonicalSymbol,
    ContractModel,
    EpochMs,
    PositiveFloat,
)


class Trade(ContractModel):
    """One executed trade, normalized across exchanges. Dedupe key: `(exchange, symbol, trade_id)`."""

    exchange: Literal["binance", "coinbase"]
    symbol: CanonicalSymbol  # canonical, e.g. BTC-USDT
    trade_id: str  # exchange trade id, as a string (Binance ints, Coinbase strings)
    price: PositiveFloat
    qty: PositiveFloat
    side: Literal["buy", "sell"]  # taker side
    event_ts: EpochMs  # exchange timestamp, UTC ms
    ingest_ts: EpochMs  # when the producer received it, UTC ms
    schema_version: int = SCHEMA_VERSION
