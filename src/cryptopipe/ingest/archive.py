# Owner: D (Batch and data) | Task: A4 | Status: stub, signatures frozen by the bootstrap
"""Downloader for the Binance public archive (data.binance.vision): daily trades, monthly 1m klines.

Idempotent and resumable: files already present and verified are skipped. Every zip is checked against its
`.CHECKSUM` (SHA256) file. Since 2025-01-01 spot archive timestamps are microseconds (see docs/contracts.md).
Sample of the unzipped CSV format: tests/fixtures/archive_trades_sample.csv.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

BASE_URL = "https://data.binance.vision/data/spot"

Kind = Literal["trades", "klines"]


def archive_url(symbol: str, kind: Kind, day: date) -> str:
    """URL of one archive file, e.g. `.../daily/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.zip`
    or `.../monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-09.zip`."""
    raise NotImplementedError("Task A4 – owner D")


def download(symbols: list[str], kind: Kind, start: date, end: date, dest: Path) -> list[Path]:
    """Download, verify and unzip archives for Binance `symbols` between `start` and `end` (inclusive).

    Returns the paths of the extracted CSV files (existing ones included).
    """
    raise NotImplementedError("Task A4 – owner D")


def verify_checksum(path: Path) -> bool:
    """True if `path` matches the SHA256 in `path.with_suffix(path.suffix + '.CHECKSUM')`."""
    raise NotImplementedError("Task A4 – owner D")
