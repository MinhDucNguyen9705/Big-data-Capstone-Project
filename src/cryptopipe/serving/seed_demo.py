# Owner: E. Synthetic, isolated serving demo; never writes the live crypto keyspace.
"""Print CQL for synthetic candles in crypto_demo; pipe explicitly into cqlsh to load."""

from __future__ import annotations

import argparse
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptopipe import contracts

DEMO_KEYSPACE = "crypto_demo"


def render(schema: str, now: datetime, minutes: int = 60) -> str:
    if not 1 <= minutes <= 1440:
        raise ValueError("minutes must be between 1 and 1440")
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(UTC).replace(second=0, microsecond=0)
    statements = ["-- SYNTHETIC serving demo. All writes are isolated in crypto_demo."]
    statements.append(re.sub(r"\bcrypto\b", DEMO_KEYSPACE, schema))
    for i in range(minutes):
        start = now - timedelta(minutes=minutes - i)
        end = start + timedelta(minutes=1)
        price = 60000 + i * 3 + (i % 7 - 3) * 10
        close = price + (8 if i % 2 else -6)
        high, low = max(price, close) + 5, min(price, close) - 5
        volume, trades = 1 + (i % 5) / 10, 10 + i % 20
        vwap = (price + close) / 2
        statements.append(
            f"INSERT INTO {DEMO_KEYSPACE}.{contracts.CASSANDRA_OHLCV_1M} "
            "(symbol, day, window_start, exchange, window_end, open, high, low, close, volume, "
            "quote_volume, trades, vwap, updated_at) VALUES "
            f"('BTC-USDT', '{start.date()}', '{start.isoformat()}', 'binance', '{end.isoformat()}', "
            f"{price}, {high}, {low}, {close}, {volume}, {volume * vwap}, {trades}, {vwap}, "
            f"'{now.isoformat()}');"
        )
    last_event = now - timedelta(milliseconds=1)
    statements.append(
        f"INSERT INTO {DEMO_KEYSPACE}.{contracts.CASSANDRA_LATEST_PRICE} "
        "(symbol, exchange, price, qty, event_ts) VALUES "
        f"('BTC-USDT', 'binance', {close}, 0.01, '{last_event.isoformat()}');"
    )
    return "\n".join(statements) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, default=Path("cql/schema.cql"))
    parser.add_argument("--minutes", type=int, default=60)
    args = parser.parse_args()
    print(render(args.schema.read_text(), datetime.now(UTC), args.minutes), end="")


if __name__ == "__main__":
    main()
