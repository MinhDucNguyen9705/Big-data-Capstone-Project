# Owner: B (Ingestion) | Task: A3 | Status: implemented
"""Kafka producer factory, send helpers, the dead-letter queue (`dlq.ingest`) and the producers' Prometheus metrics.

`producer=None` means dry run: `send` prints the JSON to stdout and `send_dlq` logs a warning.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import Counter, Gauge

from cryptopipe.common.logging import get_logger
from cryptopipe.config import Settings
from cryptopipe.contracts import TOPIC_DLQ, Topic
from cryptopipe.schemas import DlqRecord
from cryptopipe.schemas.common import ContractModel, now_ms

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message, Producer

log = get_logger(__name__)

MESSAGES_SENT = Counter("crypto_ingest_messages_sent", "Messages handed to the Kafka producer", ["topic"])
DLQ_SENT = Counter("crypto_ingest_dlq", "Invalid payloads sent to dlq.ingest", ["source"])
DELIVERY_ERRORS = Counter("crypto_ingest_delivery_errors", "Messages Kafka failed to deliver", ["topic"])
RECONNECTS = Counter("crypto_ingest_reconnects", "WebSocket reconnects", ["source", "reason"])
GAP_BACKFILLED = Counter("crypto_ingest_gap_trades_backfilled", "Trades fetched from REST on reconnect", ["source"])
EVENT_LAG = Gauge("crypto_ingest_event_lag_seconds", "ingest_ts - event_ts of the last trade", ["source"])


def make_producer(settings: Settings) -> Producer:
    """Idempotent producer: `enable.idempotence=true`, `acks=all`, `compression.type=zstd`, `linger.ms=50`.

    Bootstrap servers and security protocol come from `settings`; delivery errors are logged and counted.
    """
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "security.protocol": settings.kafka_security_protocol,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
            "error_cb": lambda err: log.error("kafka error", extra={"error": str(err)}),
        }
    )


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        DELIVERY_ERRORS.labels(msg.topic()).inc()
        log.error("delivery failed", extra={"topic": msg.topic(), "error": str(err)})


def _produce(producer: Producer, topic: str, value: str, key: str) -> None:
    try:
        producer.produce(topic, value=value, key=key, on_delivery=_on_delivery)
    except BufferError:  # local queue full: let it drain, then retry once
        producer.poll(1)
        producer.produce(topic, value=value, key=key, on_delivery=_on_delivery)
    producer.poll(0)  # serve delivery callbacks


def send(producer: Producer | None, topic: Topic, model: ContractModel, key: str | None = None) -> None:
    """Produce `model.to_json()` to `topic.name`. Key defaults to `model.symbol` (canonical symbol)."""
    value = model.to_json()
    if producer is None:
        print(value, flush=True)
        return
    _produce(producer, topic.name, value, key or model.symbol)
    MESSAGES_SENT.labels(topic.name).inc()


def send_dlq(producer: Producer | None, raw: str | bytes, reason: str, source: str) -> None:
    """Wrap an invalid payload in a `DlqRecord` and produce it to `dlq.ingest` (key = `source`)."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    DLQ_SENT.labels(source).inc()
    if producer is None:
        log.warning("dlq", extra={"source": source, "reason": reason, "raw": raw[:500]})
        return
    record = DlqRecord(source=source, reason=reason, raw=raw, ingest_ts=now_ms())
    _produce(producer, TOPIC_DLQ.name, record.to_json(), source)
