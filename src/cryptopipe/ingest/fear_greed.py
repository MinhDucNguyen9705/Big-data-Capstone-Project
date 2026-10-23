# Owner: B (Ingestion) | Task: A3 | Status: implemented
"""alternative.me Fear & Greed index poller -> compacted topic `ref.fear_greed` (daily).

Raw sample: tests/fixtures/fear_greed.json.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from cryptopipe.contracts import TOPIC_FEAR_GREED
from cryptopipe.ingest import kafka_io
from cryptopipe.schemas import FearGreed

if TYPE_CHECKING:
    from confluent_kafka import Producer

FNG_URL = "https://api.alternative.me/fng/"
SOURCE = "fear_greed"


def _parse_one(item: dict[str, Any]) -> FearGreed:
    return FearGreed(
        value=int(item["value"]),
        classification=item["value_classification"],
        ts=int(item["timestamp"]) * 1000,
    )


def parse_response(payload: dict[str, Any]) -> list[FearGreed]:
    """`{"data": [{"value": "42", "value_classification": "Fear", "timestamp": "1759968000"}, ...]}` -> models.

    Note the API gives seconds; the contract is milliseconds.
    """
    return [_parse_one(item) for item in payload["data"]]


def _get(limit: int) -> dict[str, Any]:
    r = httpx.get(FNG_URL, params={"limit": limit}, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch(limit: int = 0) -> list[FearGreed]:
    """Fetch the index history (`limit=0` = all days)."""
    return parse_response(_get(limit))


def run_once(producer: Producer | None, dry_run: bool = False) -> int:
    """Fetch and produce to `ref.fear_greed` (key = UTC day `yyyy-MM-dd`). Returns the number of messages."""
    sent = 0
    for item in _get(0)["data"]:  # whole history: the topic is compacted, re-sending a day is harmless
        try:
            fg = _parse_one(item)
            day = datetime.fromtimestamp(fg.ts / 1000, tz=UTC).strftime("%Y-%m-%d")
            kafka_io.send(producer, TOPIC_FEAR_GREED, fg, key=day)
            sent += 1
        except (ValueError, KeyError, TypeError) as e:
            kafka_io.send_dlq(producer, json.dumps(item), f"{type(e).__name__}: {e}", SOURCE)
    if producer is not None:
        producer.flush(30)
    return sent
