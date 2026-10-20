# Owner: B (Ingestion) | Task: A3 | Status: stub, signatures frozen by the bootstrap
"""CoinGecko `/coins/markets` poller -> compacted topic `ref.asset_meta` (every 10 minutes, < 10 calls/min).

Raw sample: tests/fixtures/coingecko_markets.json.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cryptopipe.schemas import AssetMeta

if TYPE_CHECKING:
    from confluent_kafka import Producer

MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


def parse_markets(payload: list[dict[str, Any]], snapshot_ts: int) -> list[AssetMeta]:
    """Response of `/coins/markets` -> one `AssetMeta` per tracked asset (untracked ids are skipped)."""
    raise NotImplementedError("Task A3 – owner B")


def fetch() -> list[AssetMeta]:
    """Call the API for every tracked CoinGecko id and parse the response."""
    raise NotImplementedError("Task A3 – owner B")


def run_once(producer: Producer | None, dry_run: bool = False) -> int:
    """Fetch and produce to `ref.asset_meta` (key = canonical symbol). Returns the number of messages."""
    raise NotImplementedError("Task A3 – owner B")
