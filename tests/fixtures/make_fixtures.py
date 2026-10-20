"""Seeded generator for the synthetic fixtures (the raw exchange samples come from capture_live.py).

    python tests/fixtures/make_fixtures.py           # (re)write the files
    python tests/fixtures/make_fixtures.py --check   # verify the committed files match the seed

Outputs:
* trades_normalized.jsonl: 500 `Trade` JSON lines (contract format) for BTC-USDT, ETH-USDT and SOL-USDT over
  10 minutes across UTC midnight (2 dates), in *arrival* order (sorted by ingest_ts), including
  5 exact duplicates, 5 out-of-order trades (arrive 2-10 s late), 3 late trades (arrive 90-180 s late, beyond a
  30 s watermark) and 3 invalid rows (negative price, unknown symbol, event_ts 1 h after ingest_ts).
* expected_ohlcv_1m.json: the 1m candles for the valid, deduplicated trades (batch semantics: late trades are
  included), computed with a pure-Python reference.
* archive_trades_sample.csv: 1,000 BTCUSDT rows in Binance archive format (headerless:
  id, price, qty, quote_qty, time, is_buyer_maker, is_best_match) with microsecond timestamps.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).parent
SEED = 20251009

T0_MS = 1_760_054_100_000  # 2025-10-09T23:55:00Z
SPAN_MS = 10 * 60 * 1000
N_LINES = 500
N_DUPLICATES = 5
N_OUT_OF_ORDER = 5
N_LATE = 3
N_INVALID = 3

SYMBOLS = {  # canonical -> (start price, price step, qty scale, decimals)
    "BTC-USDT": (62_000.0, 4.0, 0.05, 2),
    "ETH-USDT": (4_500.0, 0.5, 0.8, 2),
    "SOL-USDT": (220.0, 0.04, 6.0, 3),
}

CAPTURED = {  # file -> expected number of lines/items
    "binance_ws_trade.jsonl": 50,
    "binance_ws_kline.jsonl": 50,
    "coinbase_ws_trade.jsonl": 50,
}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def make_trades(rng: random.Random) -> tuple[list[dict], list[dict]]:
    """Return (file rows in arrival order, the valid unique trades)."""
    n_valid = N_LINES - N_DUPLICATES - N_INVALID
    price = {s: p for s, (p, *_rest) in SYMBOLS.items()}
    next_id = {}
    used_ts: set[tuple[str, int]] = set()
    trades = []
    for _ in range(n_valid):
        symbol = rng.choices(list(SYMBOLS), weights=[5, 3, 2])[0]
        exchange = "binance" if rng.random() < 0.8 else "coinbase"
        _, step, qty_scale, decimals = SYMBOLS[symbol]
        # unique event_ts per (exchange, symbol) so first/last trade of a minute is unambiguous
        while True:
            ts = T0_MS + rng.randrange(SPAN_MS)
            if (exchange + symbol, ts) not in used_ts:
                used_ts.add((exchange + symbol, ts))
                break
        key = (exchange, symbol)
        first_id = (5_205_000_000 if exchange == "binance" else 1_108_000_000) + 1000 * len(next_id)
        next_id[key] = next_id.get(key, first_id)
        trades.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "trade_id": None,  # assigned below in event-time order
                "price": None,
                "qty": round(rng.expovariate(1 / qty_scale) + 10**-decimals, 5),
                "side": rng.choice(["buy", "sell"]),
                "event_ts": ts,
                "ingest_ts": ts + rng.randint(20, 300),
                "schema_version": 1,
                "_step": step,
                "_decimals": decimals,
            }
        )
    # trade ids and a price random walk follow event time per (exchange, symbol)
    for t in sorted(trades, key=lambda t: t["event_ts"]):
        key = (t["exchange"], t["symbol"])
        t["trade_id"] = str(next_id[key])
        next_id[key] += 1
        price[t["symbol"]] = max(price[t["symbol"]] + rng.gauss(0, 1) * t["_step"], t["_step"])
        t["price"] = round(price[t["symbol"]], t["_decimals"])
    for t in trades:
        del t["_step"], t["_decimals"]

    # delayed arrivals: out-of-order (inside a 30 s watermark) and late (beyond it); not in the last minute
    candidates = [t for t in trades if t["event_ts"] < T0_MS + SPAN_MS - 4 * 60_000]
    picked = rng.sample(candidates, N_OUT_OF_ORDER + N_LATE)
    for t in picked[:N_OUT_OF_ORDER]:
        t["ingest_ts"] = t["event_ts"] + rng.randint(2_000, 10_000)
    for t in picked[N_OUT_OF_ORDER:]:
        t["ingest_ts"] = t["event_ts"] + rng.randint(90_000, 180_000)

    rows = sorted(trades, key=lambda t: (t["ingest_ts"], t["trade_id"]))

    # exact duplicates: re-delivery of the same message a few positions later
    for src in sorted(rng.sample(range(len(rows) - 10), N_DUPLICATES), reverse=True):
        rows.insert(src + rng.randint(1, 8), dict(rows[src]))

    def invalid(base: dict, **changes) -> dict:
        row = dict(base, **changes)
        row["trade_id"] = "9" + row["trade_id"]
        return row

    bases = rng.sample(trades, N_INVALID)
    bad = [
        invalid(bases[0], price=-abs(bases[0]["price"])),
        invalid(bases[1], symbol="FOO-USDT"),
        invalid(bases[2], event_ts=bases[2]["ingest_ts"] + 3_600_000),
    ]
    for row in bad:
        rows.insert(rng.randrange(len(rows)), row)
    assert len(rows) == N_LINES
    return rows, trades


def expected_ohlcv(trades: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for t in trades:
        start = t["event_ts"] - t["event_ts"] % 60_000
        groups.setdefault((t["exchange"], t["symbol"], start), []).append(t)
    out = []
    for (exchange, symbol, start), ts in sorted(groups.items()):
        ts.sort(key=lambda t: (t["event_ts"], t["trade_id"]))
        volume = sum(t["qty"] for t in ts)
        quote = sum(t["price"] * t["qty"] for t in ts)
        out.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "window_start": _iso(start),
                "window_end": _iso(start + 60_000),
                "open": ts[0]["price"],
                "high": max(t["price"] for t in ts),
                "low": min(t["price"] for t in ts),
                "close": ts[-1]["price"],
                "volume": round(volume, 10),
                "quote_volume": round(quote, 6),
                "trades": len(ts),
                "vwap": round(quote / volume, 6),
                "date": _date(start),
            }
        )
    return out


def make_archive(rng: random.Random) -> str:
    # 2025-09-01T00:00:00Z in microseconds (Binance spot archives switched to us on 2025-01-01)
    ts_us = 1_756_684_800_000_000
    trade_id = 5_205_379_727
    price = 108_246.35
    lines = []
    for _ in range(1000):
        ts_us += rng.randint(50, 120_000)
        price = round(max(price + rng.gauss(0, 1) * 2.5, 1.0), 2)
        qty = round(rng.expovariate(1 / 0.02) + 0.00001, 5)
        is_buyer_maker = rng.random() < 0.5
        lines.append(f"{trade_id},{price:.8f},{qty:.8f},{price * qty:.8f},{ts_us},{is_buyer_maker},True")
        trade_id += 1
    return "\n".join(lines) + "\n"


def build() -> dict[str, str]:
    rng = random.Random(SEED)
    rows, trades = make_trades(rng)
    return {
        "trades_normalized.jsonl": "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows),
        "expected_ohlcv_1m.json": json.dumps(expected_ohlcv(trades), indent=1) + "\n",
        "archive_trades_sample.csv": make_archive(rng),
    }


def check(files: dict[str, str]) -> list[str]:
    problems = []
    for name, content in files.items():
        path = HERE / name
        if not path.exists():
            problems.append(f"missing {name}")
        elif path.read_bytes().decode("utf-8") != content:
            problems.append(f"{name} differs from the seed; run make_fixtures.py and commit")
    for name, n in CAPTURED.items():
        path = HERE / name
        if not path.exists():
            problems.append(f"missing {name} (run capture_live.py)")
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        if len(lines) != n:
            problems.append(f"{name}: {len(lines)} lines, expected {n}")
        for line in lines:
            json.loads(line)
    for name in ("coingecko_markets.json", "fear_greed.json"):
        if not (HERE / name).exists():
            problems.append(f"missing {name} (run capture_live.py --only rest)")
    total = sum(p.stat().st_size for p in HERE.iterdir() if p.is_file())
    if total > 2_000_000:
        problems.append(f"fixtures total {total} bytes > 2 MB")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify instead of writing")
    args = parser.parse_args()
    files = build()
    if args.check:
        problems = check(files)
        for p in problems:
            print(f"FAIL {p}")
        if not problems:
            print(f"OK: {len(files)} generated and {len(CAPTURED) + 2} captured fixtures match")
        return 1 if problems else 0
    for name, content in files.items():
        (HERE / name).write_bytes(content.encode("utf-8"))
        print(f"wrote {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
