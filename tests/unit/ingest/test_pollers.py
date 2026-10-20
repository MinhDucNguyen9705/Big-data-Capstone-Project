# Owner: B | Task: A3. Remove the skip marker when implementing the CoinGecko and Fear & Greed pollers.
import json

import pytest

from cryptopipe.ingest import coingecko, fear_greed

pytestmark = pytest.mark.skip(reason="TODO A3 – owner B")


def test_parse_coingecko_markets(fixtures_dir):
    payload = json.loads((fixtures_dir / "coingecko_markets.json").read_text())
    metas = coingecko.parse_markets(payload, snapshot_ts=1_760_000_000_000)
    assert {m.symbol for m in metas} >= {"BTC-USDT", "ETH-USDT"}
    assert all(m.market_cap is None or m.market_cap > 0 for m in metas)


def test_parse_fear_greed(fixtures_dir):
    payload = json.loads((fixtures_dir / "fear_greed.json").read_text())
    values = fear_greed.parse_response(payload)
    assert len(values) == len(payload["data"])
    assert all(v.ts % 1000 == 0 and v.ts > 1_000_000_000_000 for v in values)  # seconds -> ms
