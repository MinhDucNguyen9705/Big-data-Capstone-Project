"""`Kline`: value of topic `raw.klines` (CONTRACT, see docs/contracts.md)."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from cryptopipe.schemas.common import (
    SCHEMA_VERSION,
    CanonicalSymbol,
    ContractModel,
    EpochMs,
    NonNegativeFloat,
    PositiveFloat,
)


class Kline(ContractModel):
    """One candle as published by the exchange. `raw.klines` carries closed candles only (`is_closed=True`)."""

    exchange: Literal["binance", "coinbase"]
    symbol: CanonicalSymbol
    interval: str  # "1m"
    open_ts: EpochMs
    close_ts: EpochMs
    open: PositiveFloat
    high: PositiveFloat
    low: PositiveFloat
    close: PositiveFloat
    volume: NonNegativeFloat  # base asset
    quote_volume: NonNegativeFloat
    trades: int = Field(ge=0)
    is_closed: bool
    schema_version: int = SCHEMA_VERSION

    @model_validator(mode="after")
    def _consistent(self) -> Kline:
        if self.close_ts < self.open_ts:
            raise ValueError("close_ts before open_ts")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("high/low inconsistent with open/close")
        return self
