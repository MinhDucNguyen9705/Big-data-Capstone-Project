# Owner: B (Ingestion) | Task: A3 | Status: implemented
"""Coinbase Advanced Trade WebSocket, `market_trades` channel -> `raw.trades` (`exchange="coinbase"`).

The channel is public (no JWT). The first message after subscribing is a `snapshot` of recent trades, which
re-sends trades after every reconnect; dedupe downstream absorbs it. Raw sample: tests/fixtures/coinbase_ws_trade.jsonl.

Coinbase's `side` is the MAKER side (a `BUY` prints at the best bid), so it is inverted to get the taker side
(found 2026-10-23).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from prometheus_client import start_http_server

from cryptopipe.config import settings
from cryptopipe.contracts import TOPIC_RAW_TRADES
from cryptopipe.ingest import kafka_io
from cryptopipe.ingest.ws import run_forever
from cryptopipe.schemas import Trade
from cryptopipe.schemas.common import now_ms
from cryptopipe.symbols import load_symbols

if TYPE_CHECKING:
    from confluent_kafka import Producer

WS_URL = "wss://advanced-trade-ws.coinbase.com"
SOURCE = "coinbase_ws"
TAKER_SIDE = {"BUY": "sell", "SELL": "buy"}  # Coinbase reports the maker side


async def run(symbols: list[str], producer: Producer | None, dry_run: bool = False) -> None:
    """Stream trades for `symbols` (canonical; mapped to `-USD` products) until cancelled."""
    smap = load_symbols()
    products = [p for s in symbols if (p := smap.to_exchange("coinbase", s))]

    def on_message(raw: str | bytes) -> None:
        try:
            trades = parse_message(json.loads(raw))
        except (ValueError, KeyError, TypeError) as e:  # pydantic ValidationError is a ValueError
            kafka_io.send_dlq(producer, raw, f"{type(e).__name__}: {e}", SOURCE)
            return
        for trade in trades:
            kafka_io.send(producer, TOPIC_RAW_TRADES, trade)
        if trades:
            kafka_io.EVENT_LAG.labels(SOURCE).set((trades[0].ingest_ts - trades[0].event_ts) / 1000)

    if not dry_run:
        start_http_server(settings.metrics_port)
    subscribe = [
        {"type": "subscribe", "product_ids": products, "channel": "market_trades"},
        # keeps quiet connections open (Coinbase closes them after 60-90 s without messages)
        {"type": "subscribe", "channel": "heartbeats"},
    ]
    try:
        await run_forever(WS_URL, SOURCE, on_message, subscribe=subscribe)
    finally:
        if producer is not None:
            producer.flush(10)


def _epoch_ms(rfc3339: str) -> int:
    # fromisoformat (3.11) keeps microseconds of a nanosecond timestamp
    return int(datetime.fromisoformat(rfc3339).timestamp() * 1000)


def parse_trade(trade: dict[str, Any]) -> Trade:
    """One element of `msg["events"][i]["trades"]` -> `Trade` (time is RFC 3339 with ns; side upper-case)."""
    symbol = load_symbols().to_canonical("coinbase", trade["product_id"])
    if symbol is None:
        raise ValueError(f"unknown symbol {trade['product_id']}")
    return Trade(
        exchange="coinbase",
        symbol=symbol,
        trade_id=str(trade["trade_id"]),
        price=float(trade["price"]),
        qty=float(trade["size"]),
        side=TAKER_SIDE[trade["side"]],
        event_ts=_epoch_ms(trade["time"]),
        ingest_ts=now_ms(),
    )


def parse_message(msg: dict[str, Any]) -> list[Trade]:
    """All trades of one `market_trades` message (snapshot or update); other channels -> []."""
    if msg.get("channel") != "market_trades":
        return []
    return [parse_trade(t) for e in msg["events"] for t in e.get("trades", [])]
