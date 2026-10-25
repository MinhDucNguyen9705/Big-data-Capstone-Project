"""`FearGreed`: value of compacted topic `ref.fear_greed` (CONTRACT, see cryptopipe.contracts)."""

from __future__ import annotations

from pydantic import Field

from cryptopipe.schemas.common import SCHEMA_VERSION, ContractModel, EpochMs


class FearGreed(ContractModel):
    """Daily Crypto Fear & Greed index (alternative.me). Kafka key: the UTC day `yyyy-MM-dd` of `ts`."""

    value: int = Field(ge=0, le=100)
    classification: str  # "Extreme Fear" .. "Extreme Greed"
    ts: EpochMs  # start of the day the value applies to, UTC ms
    schema_version: int = SCHEMA_VERSION
