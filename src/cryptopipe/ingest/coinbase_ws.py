# Owner: B (Ingestion) | Task: A3 | Status: stub, signatures frozen by the bootstrap
"""Coinbase Advanced Trade WebSocket, `market_trades` channel -> `raw.trades` (`exchange="coinbase"`).

The channel is public (no JWT). The first message after subscribing is a `snapshot` of recent trades, which
re-sends trades after every reconnect; dedupe downstream absorbs it. Raw sample: tests/fixtures/coinbase_ws_trade.jsonl.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cryptopipe.schemas import Trade

if TYPE_CHECKING:
    from confluent_kafka import Producer

WS_URL = "wss://advanced-trade-ws.coinbase.com"


async def run(symbols: list[str], producer: Producer | None, dry_run: bool = False) -> None:
    """Stream trades for `symbols` (canonical; mapped to `-USD` products) until cancelled."""
    raise NotImplementedError("Task A3 – owner B")


def parse_trade(trade: dict[str, Any]) -> Trade:
    """One element of `msg["events"][i]["trades"]` -> `Trade` (time is RFC 3339 with ns; side upper-case)."""
    raise NotImplementedError("Task A3 – owner B")


def parse_message(msg: dict[str, Any]) -> list[Trade]:
    """All trades of one `market_trades` message (snapshot or update); other channels -> []."""
    raise NotImplementedError("Task A3 – owner B")
