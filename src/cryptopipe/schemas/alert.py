"""`Alert`: value of topic `alerts` and rows of Cassandra `alerts` (CONTRACT, see cryptopipe.contracts)."""

from __future__ import annotations

from typing import Literal

from cryptopipe.schemas.common import SCHEMA_VERSION, CanonicalSymbol, ContractModel, EpochMs


class Alert(ContractModel):
    """A detection from the speed layer (B3), e.g. a whale burst or a price spike. Kafka key: symbol."""

    alert_id: str  # deterministic (e.g. hash of type+symbol+window) so replays upsert, not duplicate
    symbol: CanonicalSymbol
    type: str  # "whale_trade", "price_spike", "volume_burst", ...
    severity: Literal["info", "warning", "critical"]
    value: float  # the measured value that triggered the alert
    threshold: float
    event_ts: EpochMs  # event time of the triggering data
    details: dict[str, str] = {}
    schema_version: int = SCHEMA_VERSION
