"""Capture raw public samples into tests/fixtures/ (run once by hand; the outputs are committed).

    python tests/fixtures/capture_live.py            # all sources, ~20 s
    python tests/fixtures/capture_live.py --only rest

Writes binance_ws_trade.jsonl, binance_ws_kline.jsonl, coinbase_ws_trade.jsonl (50 raw messages each,
exactly as received), coingecko_markets.json and fear_greed.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import httpx
import websockets

from cryptopipe.symbols import load_symbols

HERE = Path(__file__).parent
N = 50
TIMEOUT_S = 30

SYMBOLS = load_symbols()
BINANCE = [s.lower() for s in SYMBOLS.for_exchange("binance")]
COINBASE = SYMBOLS.for_exchange("coinbase")
COINGECKO = ",".join(SYMBOLS.for_exchange("coingecko"))


async def _collect(url: str, out: Path, subscribe: dict | None = None, keep=lambda m: True) -> None:
    lines: list[str] = []
    async with websockets.connect(url, max_size=2**22, open_timeout=15) as ws:
        if subscribe:
            await ws.send(json.dumps(subscribe))
        async with asyncio.timeout(TIMEOUT_S):
            while len(lines) < N:
                raw = await ws.recv()
                if keep(json.loads(raw)):
                    lines.append(raw if isinstance(raw, str) else raw.decode())
    out.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(f"{out.name}: {len(lines)} messages")


async def capture_ws() -> None:
    base = "wss://stream.binance.com:9443/stream?streams="
    await asyncio.gather(
        _collect(base + "/".join(f"{s}@trade" for s in BINANCE), HERE / "binance_ws_trade.jsonl"),
        _collect(base + "/".join(f"{s}@kline_1m" for s in BINANCE), HERE / "binance_ws_kline.jsonl"),
        _collect(
            "wss://advanced-trade-ws.coinbase.com",
            HERE / "coinbase_ws_trade.jsonl",
            subscribe={"type": "subscribe", "product_ids": COINBASE, "channel": "market_trades"},
            # skip subscription acks/heartbeats and the large initial snapshot, keep live trade updates
            keep=lambda m: (
                m.get("channel") == "market_trades" and any(e.get("type") == "update" for e in m.get("events", []))
            ),
        ),
    )


def _write_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8", newline="\n")


def capture_rest() -> None:
    with httpx.Client(timeout=20) as client:
        r = client.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "ids": COINGECKO},
        )
        r.raise_for_status()
        _write_json(HERE / "coingecko_markets.json", r.json())
        print(f"coingecko_markets.json: {len(r.json())} assets")
        r = client.get("https://api.alternative.me/fng/", params={"limit": 30})
        r.raise_for_status()
        _write_json(HERE / "fear_greed.json", r.json())
        print(f"fear_greed.json: {len(r.json()['data'])} days")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["ws", "rest"])
    args = parser.parse_args()
    if args.only != "rest":
        asyncio.run(capture_ws())
    if args.only != "ws":
        capture_rest()


if __name__ == "__main__":
    main()
