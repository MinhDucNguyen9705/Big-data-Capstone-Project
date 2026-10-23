# Owner: B | Task: A3. Tests for cryptopipe.ingest.coinbase_ws.
import json

from cryptopipe.ingest import coinbase_ws


def test_parse_message_fixture(fixtures_dir):
    total = 0
    for line in (fixtures_dir / "coinbase_ws_trade.jsonl").read_text().splitlines():
        msg = json.loads(line)
        trades = coinbase_ws.parse_message(msg)
        assert len(trades) == sum(len(e.get("trades", [])) for e in msg["events"])
        total += len(trades)
        for t in trades:
            assert t.exchange == "coinbase"
            assert t.symbol.endswith("-USDT")  # BTC-USD -> BTC-USDT
            assert t.side in ("buy", "sell")
    assert total > 0


def test_side_is_inverted_to_taker():
    # Coinbase reports the maker side: a BUY prints at the bid, i.e. the taker sold.
    trade = {"product_id": "BTC-USD", "trade_id": "1", "price": "1", "size": "1",
             "time": "2026-10-20T11:43:55.302780241Z", "side": "BUY"}  # fmt: skip
    t = coinbase_ws.parse_trade(trade)
    assert t.side == "sell"
    assert t.event_ts == 1_792_496_635_302
