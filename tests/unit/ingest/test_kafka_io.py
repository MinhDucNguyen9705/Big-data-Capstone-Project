# Owner: B | Task: A3. Tests for cryptopipe.ingest.kafka_io.
import json

from cryptopipe import contracts
from cryptopipe.ingest import kafka_io
from cryptopipe.schemas import Trade


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value=None, key=None, **kwargs):
        self.sent.append((topic, key, value))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0


TRADE = Trade(
    exchange="binance", symbol="BTC-USDT", trade_id="1", price=1.0, qty=1.0, side="buy",
    event_ts=1_760_000_000_000, ingest_ts=1_760_000_000_100,
)  # fmt: skip


def test_send_uses_canonical_symbol_key():
    p = FakeProducer()
    kafka_io.send(p, contracts.TOPIC_RAW_TRADES, TRADE)
    topic, key, value = p.sent[0]
    assert topic == "raw.trades"
    assert key in ("BTC-USDT", b"BTC-USDT")
    assert Trade.model_validate_json(value) == TRADE


def test_send_dlq_wraps_payload():
    p = FakeProducer()
    kafka_io.send_dlq(p, '{"bad": 1}', reason="unknown symbol", source="binance_ws")
    topic, _key, value = p.sent[0]
    assert topic == "dlq.ingest"
    assert json.loads(value)["reason"] == "unknown symbol"
