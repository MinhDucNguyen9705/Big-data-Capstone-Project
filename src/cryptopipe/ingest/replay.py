# Owner: D (Batch and data) | Task: A4 | Status: stub, signatures frozen by the bootstrap
"""Replay archive trade CSVs into `raw.trades` at N x real time or a fixed rate (load tests, busy demos)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from confluent_kafka import Producer


def replay(
    path: Path,
    producer: Producer | None,
    speed: float = 10.0,
    rate: float | None = None,
    dry_run: bool = False,
    rebase_time: bool = False,
) -> int:
    """Read archive CSVs under `path` in timestamp order and produce `Trade` messages.

    `speed` = multiple of real time; `rate` = fixed messages per second (overrides `speed`).
    `rebase_time=True` shifts `event_ts` so the first trade is "now". `dry_run` prints instead of producing.
    Returns the number of trades sent.
    """
    raise NotImplementedError("Task A4 – owner D")
