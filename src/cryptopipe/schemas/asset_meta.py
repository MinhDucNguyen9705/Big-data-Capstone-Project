"""`AssetMeta`: value of compacted topic `ref.asset_meta` (CONTRACT, see docs/contracts.md)."""

from __future__ import annotations

from cryptopipe.schemas.common import SCHEMA_VERSION, CanonicalSymbol, ContractModel, EpochMs


class AssetMeta(ContractModel):
    """Asset metadata snapshot from CoinGecko `/coins/markets`. Kafka key: canonical symbol."""

    coingecko_id: str  # e.g. "bitcoin"
    symbol: CanonicalSymbol
    name: str
    categories: list[str] = []
    market_cap: float | None = None  # USD
    circulating_supply: float | None = None
    snapshot_ts: EpochMs
    schema_version: int = SCHEMA_VERSION
