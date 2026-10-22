# Owner: D (Batch and data) | Task: A4 | Status: implemented
"""Downloader for the Binance public archive (data.binance.vision): daily trades, monthly 1m klines.

Idempotent and resumable: files already present and verified are skipped. Every zip is checked against its
`.CHECKSUM` (SHA256) file. Since 2025-01-01 spot archive timestamps are microseconds (see docs/contracts.md).
Sample of the unzipped CSV format: tests/fixtures/archive_trades_sample.csv.

Layout under `dest`: `<kind>/<SYMBOL>/<SYMBOL>-trades-2025-09-01.csv`, `klines/<SYMBOL>/<SYMBOL>-1m-2025-09.csv`.
A month whose monthly kline file is not published yet (the current month) falls back to daily kline files.
An `hdfs://` dest stages each file locally, then copies it with `hdfs dfs -put` (needs the `hdfs` client).
"""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import httpx

from cryptopipe.common.logging import get_logger

BASE_URL = "https://data.binance.vision/data/spot"
KLINE_INTERVAL = "1m"
ATTEMPTS = 3  # per file: network errors, 5xx and checksum mismatches are retried

Kind = Literal["trades", "klines"]
log = get_logger(__name__)


class ArchiveError(RuntimeError):
    """A file could not be downloaded and verified after `ATTEMPTS` tries."""


def archive_url(symbol: str, kind: Kind, day: date) -> str:
    """URL of one archive file, e.g. `.../daily/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.zip`
    or `.../monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-09.zip`."""
    symbol = symbol.upper()
    if kind == "trades":
        return f"{BASE_URL}/daily/trades/{symbol}/{symbol}-trades-{day:%Y-%m-%d}.zip"
    if kind == "klines":
        return f"{BASE_URL}/monthly/klines/{symbol}/{KLINE_INTERVAL}/{symbol}-{KLINE_INTERVAL}-{day:%Y-%m}.zip"
    raise ValueError(f"unknown archive kind {kind!r}")


def daily_klines_url(symbol: str, day: date) -> str:
    symbol = symbol.upper()
    return f"{BASE_URL}/daily/klines/{symbol}/{KLINE_INTERVAL}/{symbol}-{KLINE_INTERVAL}-{day:%Y-%m-%d}.zip"


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def months(start: date, end: date) -> list[date]:
    """First day of every month touching [start, end]."""
    out, current = [], start.replace(day=1)
    while current <= end:
        out.append(current)
        current = (current + timedelta(days=32)).replace(day=1)
    return out


def verify_checksum(path: Path) -> bool:
    """True if `path` matches the SHA256 in `path.with_suffix(path.suffix + '.CHECKSUM')`."""
    checksum_file = path.with_suffix(path.suffix + ".CHECKSUM")
    if not path.exists() or not checksum_file.exists():
        return False
    expected = checksum_file.read_text(encoding="utf-8").split()[0].lower()
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest() == expected


class _NotFound(Exception):
    pass


def _get(client: httpx.Client, url: str, dest: Path) -> None:
    """Stream `url` to `dest` atomically (via `.part`). Raises _NotFound on 404."""
    part = dest.with_name(dest.name + ".part")
    with client.stream("GET", url) as response:
        if response.status_code == 404:
            raise _NotFound(url)
        response.raise_for_status()
        with part.open("wb") as f:
            for chunk in response.iter_bytes(1 << 20):
                f.write(chunk)
    part.replace(dest)


def _fetch_one(client: httpx.Client, url: str, folder: Path) -> Path | None:
    """Download, verify and unzip one archive file into `folder`. Returns the CSV path, None if not published."""
    zip_path = folder / url.rsplit("/", 1)[-1]
    csv_path = zip_path.with_suffix(".csv")
    if csv_path.exists():  # only created by the atomic rename below, after the checksum passed
        return csv_path
    folder.mkdir(parents=True, exist_ok=True)
    checksum_path = zip_path.with_suffix(".zip.CHECKSUM")

    for attempt in range(1, ATTEMPTS + 1):
        try:
            if not verify_checksum(zip_path):  # resume: a verified zip from an interrupted run is reused
                _get(client, url + ".CHECKSUM", checksum_path)
                _get(client, url, zip_path)
            if verify_checksum(zip_path):
                break
            log.warning("checksum mismatch", extra={"url": url, "attempt": attempt})
            zip_path.unlink(missing_ok=True)
        except _NotFound:
            checksum_path.unlink(missing_ok=True)
            return None
        except httpx.HTTPError as exc:
            log.warning("download failed", extra={"url": url, "attempt": attempt, "error": str(exc)})
        time.sleep(min(2**attempt, 30) if attempt < ATTEMPTS else 0)
    else:
        raise ArchiveError(f"{url}: not verified after {ATTEMPTS} attempts")

    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".csv"))
        part = csv_path.with_name(csv_path.name + ".part")
        with zf.open(member) as src, part.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
    part.replace(csv_path)
    zip_path.unlink()  # the CSV is what we keep; the .CHECKSUM stays as provenance
    log.info("downloaded", extra={"url": url, "csv": str(csv_path), "bytes": csv_path.stat().st_size})
    return csv_path


def _hdfs(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["hdfs", "dfs", *args], check=False, capture_output=True, text=True)


def _hdfs_uri(dest: Path | str) -> str | None:
    """`hdfs://nn:8020/x` survives `Path()` as `hdfs:/nn:8020/x`; restore the URI. None for local paths."""
    text = str(dest)
    return re.sub(r"^hdfs:/+", "hdfs://", text).rstrip("/") if text.startswith("hdfs:") else None


def _jobs(symbols: list[str], kind: Kind, start: date, end: date) -> list[tuple[str, date]]:
    """(symbol, period) for every file to fetch: one per day for trades, one per month for klines."""
    periods = days(start, end) if kind == "trades" else months(start, end)
    return [(s, p) for s in symbols for p in periods]


def download(
    symbols: list[str],
    kind: Kind,
    start: date,
    end: date,
    dest: Path,
    client: httpx.Client | None = None,
    workers: int = 4,
) -> list[Path]:
    """Download, verify and unzip archives for Binance `symbols` between `start` and `end` (inclusive).

    Returns the paths of the extracted CSV files (existing ones included). Files that are not published (404:
    symbol not listed yet, day in the future) are skipped with a warning. For an `hdfs://` dest the returned
    paths are the paths inside HDFS (without the scheme).
    """
    if kind not in ("trades", "klines"):
        raise ValueError(f"unknown archive kind {kind!r}")
    if end < start:
        raise ValueError(f"end {end} before start {start}")
    symbols = [s.upper() for s in symbols]
    hdfs_root = _hdfs_uri(dest)
    staging = Path(tempfile.mkdtemp(prefix="crypto-archive-")) if hdfs_root else Path(dest)
    own_client = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(60.0, connect=15.0), follow_redirects=True)

    def fetch(job: tuple[str, date]) -> list[Path]:
        symbol, period = job
        url = archive_url(symbol, kind, period)
        folder = staging / kind / symbol
        if hdfs_root:
            target = f"{hdfs_root}/{kind}/{symbol}/{url.rsplit('/', 1)[-1].replace('.zip', '.csv')}"
            if _hdfs("-test", "-e", target).returncode == 0:
                return [Path(urlparse(target).path)]
        csv = _fetch_one(client, url, folder)
        found = [csv] if csv else []
        if csv is None and kind == "klines":  # monthly file not published yet: use the daily files
            next_month = (period + timedelta(days=32)).replace(day=1)
            month_days = days(max(start, period), min(end, next_month - timedelta(days=1)))
            found = [p for p in (_fetch_one(client, daily_klines_url(symbol, d), folder) for d in month_days) if p]
        if not found:
            log.warning("not published, skipped", extra={"url": url})
        if not hdfs_root:
            return found
        out = []
        for path in found:
            target = f"{hdfs_root}/{kind}/{symbol}/{path.name}"
            _hdfs("-mkdir", "-p", target.rsplit("/", 1)[0])
            result = _hdfs("-put", "-f", str(path), target)
            if result.returncode != 0:
                raise ArchiveError(f"hdfs dfs -put {path} {target} failed: {result.stderr.strip()}")
            path.unlink()
            out.append(Path(urlparse(target).path))
        return out

    try:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            results = list(pool.map(fetch, _jobs(symbols, kind, start, end)))
    finally:
        if own_client:
            client.close()
        if hdfs_root:
            shutil.rmtree(staging, ignore_errors=True)
    return sorted(p for paths in results for p in paths)
