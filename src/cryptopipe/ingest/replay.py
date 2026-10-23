# Owner: D (Batch and data) | Task: A4 | Status: implemented
"""Replay archive trade CSVs into `raw.trades` at N x real time or a fixed rate (load tests, busy demos).

Files are merged in timestamp order (each archive file is already sorted, so a k-way merge streams any number of
files without loading them). The symbol of a file comes from its name (`BTCUSDT-trades-2025-09-01.csv`) or its
folder (`<dest>/trades/BTCUSDT/`, the downloader layout). Rows that do not validate as `Trade` go to `dlq.ingest`.
Only the `confluent_kafka.Producer` API (`produce`, `poll`, `flush`) is used, with key = canonical symbol.
"""

from __future__ import annotations

import csv
import heapq
import os
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cryptopipe.common.logging import get_logger
from cryptopipe.contracts import TOPIC_DLQ, TOPIC_RAW_TRADES
from cryptopipe.schemas import DlqRecord, Trade
from cryptopipe.schemas.common import now_ms
from cryptopipe.symbols import load_symbols

if TYPE_CHECKING:
    from confluent_kafka import Producer

DLQ_SOURCE = "replay"
# Fallback for files whose name and folder carry no symbol, e.g. tests/fixtures/archive_trades_sample.csv
DEFAULT_SYMBOL = "BTC-USDT"
PROGRESS_EVERY = 100_000
log = get_logger(__name__)


def to_ms(value: int) -> int:
    """Epoch in s/ms/us/ns -> ms by magnitude (same thresholds as `transforms.archive.epoch_to_ms`)."""
    if value < 10**11:
        return value * 1000
    if value < 10**14:
        return value
    if value < 10**17:
        return value // 1000
    return value // 1_000_000


def csv_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(path.rglob("*.csv"))
    raise FileNotFoundError(path)


def symbol_for(file: Path, default: str | None = DEFAULT_SYMBOL) -> str | None:
    """Canonical symbol from `BTCUSDT-trades-...csv` or from the parent folder `BTCUSDT`; else `default`."""
    symbols = load_symbols()
    for name in (file.name.split("-", 1)[0], file.parent.name):
        canonical = symbols.to_canonical("binance", name)
        if canonical:
            return canonical
    if default:
        log.warning("no symbol in file name or folder, using default", extra={"file": str(file), "symbol": default})
    return default


Row = tuple[int, int, str, list[str], str]  # (event_ts ms, trade id, symbol, CSV fields, raw line)


def _rows(file: Path, symbol: str, on_bad: Callable[[str, str], None]) -> Iterator[Row]:
    with file.open(newline="", encoding="utf-8") as f:
        for fields in csv.reader(f):
            if not fields:
                continue
            raw = ",".join(fields)
            try:
                yield to_ms(int(fields[4])), int(fields[0]), symbol, fields, raw
            except (ValueError, IndexError):
                if fields[0].strip().lower() != "id":  # a header line is not an error
                    on_bad(raw, f"unparseable archive row in {file.name}")


def to_trade(fields: list[str], symbol: str, event_ts: int, ingest_ts: int) -> Trade:
    """Archive CSV fields -> `Trade` (side = "sell" when the buyer is the maker)."""
    return Trade(
        exchange="binance",
        symbol=symbol,
        trade_id=fields[0],
        price=float(fields[1]),
        qty=float(fields[2]),
        side="sell" if fields[5].strip().lower() == "true" else "buy",
        event_ts=event_ts,
        ingest_ts=ingest_ts,
    )


def _produce(producer: Producer, topic: str, key: str, value: str) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=value)
            break
        except BufferError:  # local queue full: let deliveries drain
            producer.poll(0.5)
    producer.poll(0)


def _print(line: str, stream=None) -> bool:
    """Print a dry-run line; False when the reader went away (`crypto replay ... --dry-run | head`)."""
    try:
        print(line, file=stream or sys.stdout, flush=False)
        return True
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())  # no second error at interpreter exit
        return False


def replay(
    path: Path,
    producer: Producer | None,
    speed: float = 10.0,
    rate: float | None = None,
    dry_run: bool = False,
    rebase_time: bool = False,
    symbol: str | None = None,
) -> int:
    """Read archive CSVs under `path` in timestamp order and produce `Trade` messages.

    `speed` = multiple of real time; `rate` = fixed messages per second (overrides `speed`).
    `rebase_time=True` shifts `event_ts` so the first trade is "now". `dry_run` prints instead of producing.
    `symbol` (canonical) overrides the symbol taken from file names. A `speed` <= 0 replays as fast as possible.
    Returns the number of trades sent.
    """
    if not dry_run and producer is None:
        raise ValueError("a producer is required unless dry_run=True")
    if rate is not None and rate <= 0:
        raise ValueError("rate must be > 0")
    if symbol is not None and symbol not in load_symbols():
        raise ValueError(f"unknown canonical symbol {symbol!r}")

    def on_bad(raw: str, reason: str) -> None:
        nonlocal dlq
        dlq += 1
        record = DlqRecord(source=DLQ_SOURCE, reason=reason, raw=raw, ingest_ts=now_ms())
        if dry_run:
            _print(f"DLQ {record.to_json()}", sys.stderr)
        else:
            _produce(producer, TOPIC_DLQ.name, DLQ_SOURCE, record.to_json())

    dlq = sent = 0
    files = csv_files(Path(path))
    streams = [_rows(f, symbol or symbol_for(f), on_bad) for f in files]
    merged = heapq.merge(*streams, key=lambda row: (row[0], row[1]))

    first_ts: int | None = None
    offset = 0
    started = time.monotonic()
    for event_ts, _id, sym, fields, raw in merged:
        if first_ts is None:
            first_ts = event_ts
            offset = now_ms() - first_ts if rebase_time else 0
        # Pacing: wait until this row's due time (relative to the first row) on the wall clock.
        if rate is not None:
            due = sent / rate
        elif speed > 0:
            due = (event_ts - first_ts) / 1000 / speed
        else:
            due = 0.0
        ahead = due - (time.monotonic() - started)
        if ahead > 0.001:
            time.sleep(ahead)

        try:
            trade = to_trade(fields, sym, event_ts + offset, now_ms())
        except (ValidationError, ValueError, IndexError) as exc:
            on_bad(raw, f"invalid trade: {str(exc).splitlines()[0]}")
            continue
        if dry_run:
            if not _print(trade.to_json()):
                break
        else:
            _produce(producer, TOPIC_RAW_TRADES.name, trade.symbol, trade.to_json())
        sent += 1
        if sent % PROGRESS_EVERY == 0:
            log.info("replay progress", extra={"sent": sent, "dlq": dlq, "event_ts": event_ts})

    if producer is not None and not dry_run:
        producer.flush()
    log.info(
        "replay done",
        extra={"files": len(files), "sent": sent, "dlq": dlq, "seconds": round(time.monotonic() - started, 1)},
    )
    return sent
