# Owner: B (Ingestion) | Task: A3 | Status: stub, signatures frozen by the bootstrap
"""Binance combined WebSocket stream (`<sym>@trade` + `<sym>@kline_1m`) -> `raw.trades` / `raw.klines`.

Spec (docs/implementation-plan.md, A3): reconnect proactively at ~23 h and on errors with exponential backoff
(1 s -> 60 s, jitter); after a reconnect, fill the gap from REST using the last trade id per symbol; export
Prometheus metrics on `settings.metrics_port`. Raw samples: tests/fixtures/binance_ws_{trade,kline}.jsonl.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cryptopipe.schemas import Kline, Trade

if TYPE_CHECKING:
    from confluent_kafka import Producer

STREAM_URL = "wss://stream.binance.com:9443/stream"


async def run(symbols: list[str], producer: Producer | None, dry_run: bool = False) -> None:
    """Stream trades and closed 1m klines for `symbols` (canonical) until cancelled.

    `dry_run=True` prints validated messages to stdout instead of producing (producer may be None).
    """
    raise NotImplementedError("Task A3 – owner B")


def parse_trade(msg: dict[str, Any]) -> Trade:
    """Combined-stream message `{"stream": "btcusdt@trade", "data": {"e": "trade", ...}}` -> `Trade`.

    Side is the taker side: `m=true` (buyer is maker) -> "sell".
    """
    raise NotImplementedError("Task A3 – owner B")


def parse_kline(msg: dict[str, Any]) -> Kline:
    """Combined-stream message `{"stream": "btcusdt@kline_1m", "data": {"e": "kline", "k": {...}}}` -> `Kline`."""
    raise NotImplementedError("Task A3 – owner B")
