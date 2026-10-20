# Owner: B (Ingestion) | Task: A3 | Status: stub, signatures frozen by the bootstrap
"""Kafka producer factory, send helpers and the dead-letter queue (`dlq.ingest`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cryptopipe.config import Settings
from cryptopipe.contracts import Topic
from cryptopipe.schemas.common import ContractModel

if TYPE_CHECKING:
    from confluent_kafka import Producer


def make_producer(settings: Settings) -> Producer:
    """Idempotent producer: `enable.idempotence=true`, `acks=all`, `compression.type=zstd`, `linger.ms=50`.

    Bootstrap servers and security protocol come from `settings`; delivery errors are logged and counted.
    """
    raise NotImplementedError("Task A3 – owner B")


def send(producer: Producer, topic: Topic, model: ContractModel, key: str | None = None) -> None:
    """Produce `model.to_json()` to `topic.name`. Key defaults to `model.symbol` (canonical symbol)."""
    raise NotImplementedError("Task A3 – owner B")


def send_dlq(producer: Producer, raw: str | bytes, reason: str, source: str) -> None:
    """Wrap an invalid payload in a `DlqRecord` and produce it to `dlq.ingest` (key = `source`)."""
    raise NotImplementedError("Task A3 – owner B")
