# Owner: B | Task: A3. Remove the skip marker when implementing cryptopipe.ingest.coinbase_ws.
import json

import pytest

from cryptopipe.ingest import coinbase_ws

pytestmark = pytest.mark.skip(reason="TODO A3 – owner B")


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
