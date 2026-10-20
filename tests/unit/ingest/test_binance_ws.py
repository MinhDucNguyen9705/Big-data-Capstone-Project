# Owner: B | Task: A3. Remove the skip marker when implementing cryptopipe.ingest.binance_ws.
import json

import pytest

from cryptopipe.ingest import binance_ws

pytestmark = pytest.mark.skip(reason="TODO A3 – owner B")


def _messages(fixtures_dir, name):
    return [json.loads(line) for line in (fixtures_dir / name).read_text().splitlines()]


def test_parse_trade_fixture(fixtures_dir):
    for msg in _messages(fixtures_dir, "binance_ws_trade.jsonl"):
        t = binance_ws.parse_trade(msg)
        data = msg["data"]
        assert t.exchange == "binance"
        assert t.symbol.endswith("-USDT")
        assert t.trade_id == str(data["t"])
        assert t.event_ts == data["T"]
        assert t.price == float(data["p"]) and t.qty == float(data["q"])
        assert t.side == ("sell" if data["m"] else "buy")


def test_parse_kline_fixture(fixtures_dir):
    for msg in _messages(fixtures_dir, "binance_ws_kline.jsonl"):
        k = binance_ws.parse_kline(msg)
        assert k.interval == "1m"
        assert k.open_ts == msg["data"]["k"]["t"]
        assert k.is_closed == msg["data"]["k"]["x"]
