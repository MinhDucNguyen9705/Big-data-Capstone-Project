# Owner: B (Ingestion) | Task: A3 | Status: stub, signatures frozen by the bootstrap
"""alternative.me Fear & Greed index poller -> compacted topic `ref.fear_greed` (daily).

Raw sample: tests/fixtures/fear_greed.json.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cryptopipe.schemas import FearGreed

if TYPE_CHECKING:
    from confluent_kafka import Producer

FNG_URL = "https://api.alternative.me/fng/"


def parse_response(payload: dict[str, Any]) -> list[FearGreed]:
    """`{"data": [{"value": "42", "value_classification": "Fear", "timestamp": "1759968000"}, ...]}` -> models.

    Note the API gives seconds; the contract is milliseconds.
    """
    raise NotImplementedError("Task A3 – owner B")


def fetch(limit: int = 0) -> list[FearGreed]:
    """Fetch the index history (`limit=0` = all days)."""
    raise NotImplementedError("Task A3 – owner B")


def run_once(producer: Producer | None, dry_run: bool = False) -> int:
    """Fetch and produce to `ref.fear_greed` (key = UTC day `yyyy-MM-dd`). Returns the number of messages."""
    raise NotImplementedError("Task A3 – owner B")
