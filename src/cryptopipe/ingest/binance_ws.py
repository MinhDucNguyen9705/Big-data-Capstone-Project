# Owner: B (Ingestion) | Task: A3 | Status: implemented
"""Binance combined WebSocket stream (`<sym>@trade` + `<sym>@kline_1m`) -> `raw.trades` / `raw.klines`.

Spec: reconnect proactively at ~23 h and on errors with exponential backoff
(1 s -> 60 s, jitter); after a reconnect, fill the gap from REST using the last trade id per symbol; export
Prometheus metrics on `settings.metrics_port`. Raw samples: tests/fixtures/binance_ws_{trade,kline}.jsonl.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
from prometheus_client import start_http_server

from cryptopipe.common.logging import get_logger
from cryptopipe.config import settings
from cryptopipe.contracts import TOPIC_RAW_KLINES, TOPIC_RAW_TRADES
from cryptopipe.ingest import kafka_io
from cryptopipe.ingest.ws import run_forever
from cryptopipe.schemas import Kline, Trade
from cryptopipe.schemas.common import now_ms
from cryptopipe.symbols import load_symbols

if TYPE_CHECKING:
    from confluent_kafka import Producer

STREAM_URL = "wss://stream.binance.com:9443/stream"
REST_URL = "https://api.binance.com"
SOURCE = "binance_ws"
MAX_AGE_S = 23 * 3600  # Binance drops connections at 24 h
BACKFILL_PAGE = 1000  # historicalTrades max limit; keyless, weight 25 per call
BACKFILL_MAX_PAGES = 5  # per symbol and reconnect; a longer gap is left to the archive backfill (A4)

log = get_logger(__name__)


def _canonical(binance_symbol: str) -> str:
    symbol = load_symbols().to_canonical("binance", binance_symbol)
    if symbol is None:
        raise ValueError(f"unknown symbol {binance_symbol}")
    return symbol


async def run(symbols: list[str], producer: Producer | None, dry_run: bool = False) -> None:
    """Stream trades and closed 1m klines for `symbols` (canonical) until cancelled.

    `dry_run=True` prints validated messages to stdout instead of producing (producer may be None).
    """
    smap = load_symbols()
    names = [n.lower() for s in symbols if (n := smap.to_exchange("binance", s))]
    url = STREAM_URL + "?streams=" + "/".join(f"{n}@trade/{n}@kline_1m" for n in names)
    last_ids: dict[str, int] = {}  # Binance symbol -> last trade id seen

    def on_message(raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
            if msg["stream"].endswith("@trade"):
                _emit_trade(msg, producer, last_ids)
            elif msg["data"]["k"]["x"]:  # closed candles only
                kafka_io.send(producer, TOPIC_RAW_KLINES, parse_kline(msg))
        except (ValueError, KeyError, TypeError) as e:  # pydantic ValidationError is a ValueError
            kafka_io.send_dlq(producer, raw, f"{type(e).__name__}: {e}", SOURCE)

    async def on_connect() -> None:
        if last_ids:
            await _backfill(last_ids, producer)

    if not dry_run:
        start_http_server(settings.metrics_port)
    try:
        await run_forever(url, SOURCE, on_message, on_connect=on_connect, max_age_s=MAX_AGE_S)
    finally:
        if producer is not None:
            producer.flush(10)


def _emit_trade(msg: dict[str, Any], producer: Producer | None, last_ids: dict[str, int]) -> None:
    trade = parse_trade(msg)
    kafka_io.send(producer, TOPIC_RAW_TRADES, trade)
    last_ids[msg["data"]["s"]] = int(trade.trade_id)
    kafka_io.EVENT_LAG.labels(SOURCE).set((trade.ingest_ts - trade.event_ts) / 1000)


async def _backfill(last_ids: dict[str, int], producer: Producer | None) -> None:
    """Fetch trades after the last seen id per symbol from REST. Overlap with the new stream is deduped downstream."""
    async with httpx.AsyncClient(base_url=REST_URL, timeout=10) as client:
        for symbol, last_id in list(last_ids.items()):
            fetched = 0
            try:
                for _ in range(BACKFILL_MAX_PAGES):
                    r = await client.get(
                        "/api/v3/historicalTrades",
                        params={"symbol": symbol, "fromId": last_ids[symbol] + 1, "limit": BACKFILL_PAGE},
                    )
                    r.raise_for_status()
                    rows = r.json()
                    for row in rows:
                        msg = {"data": {"s": symbol, "t": row["id"], "p": row["price"], "q": row["qty"],
                                        "T": row["time"], "m": row["isBuyerMaker"]}}  # fmt: skip
                        try:
                            _emit_trade(msg, producer, last_ids)
                        except (ValueError, KeyError, TypeError) as e:
                            kafka_io.send_dlq(producer, json.dumps(row), f"{type(e).__name__}: {e}", SOURCE)
                    fetched += len(rows)
                    if len(rows) < BACKFILL_PAGE:
                        break
            except httpx.HTTPError as e:
                log.warning("gap backfill failed", extra={"symbol": symbol, "error": str(e)})
            kafka_io.GAP_BACKFILLED.labels(SOURCE).inc(fetched)
            log.info("gap backfilled", extra={"symbol": symbol, "from_id": last_id + 1, "trades": fetched})


def parse_trade(msg: dict[str, Any]) -> Trade:
    """Combined-stream message `{"stream": "btcusdt@trade", "data": {"e": "trade", ...}}` -> `Trade`.

    Side is the taker side: `m=true` (buyer is maker) -> "sell".
    """
    d = msg["data"]
    return Trade(
        exchange="binance",
        symbol=_canonical(d["s"]),
        trade_id=str(d["t"]),
        price=float(d["p"]),
        qty=float(d["q"]),
        side="sell" if d["m"] else "buy",
        event_ts=d["T"],
        ingest_ts=now_ms(),
    )


def parse_kline(msg: dict[str, Any]) -> Kline:
    """Combined-stream message `{"stream": "btcusdt@kline_1m", "data": {"e": "kline", "k": {...}}}` -> `Kline`."""
    k = msg["data"]["k"]
    return Kline(
        exchange="binance",
        symbol=_canonical(k["s"]),
        interval=k["i"],
        open_ts=k["t"],
        close_ts=k["T"],
        open=float(k["o"]),
        high=float(k["h"]),
        low=float(k["l"]),
        close=float(k["c"]),
        volume=float(k["v"]),
        quote_volume=float(k["q"]),
        trades=k["n"],
        is_closed=k["x"],
    )
