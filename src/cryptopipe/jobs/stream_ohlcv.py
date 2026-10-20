# Owner: C (Speed layer) | Task: A6 | Status: stub, signatures frozen by the bootstrap
"""Kafka `raw.trades` -> validated, deduped 1m OHLCV -> Cassandra (+ lake).

parse_trades -> validate_trades (bad -> quarantine) -> dedupe_trades(streaming=True) -> watermark 30 s ->
ohlcv_1m, outputMode("update"), trigger 10 s, foreachBatch -> `serving.cassandra_io.write_ohlcv` and
`write_latest_price`. Checkpoint `{settings.checkpoint_root}/stream_ohlcv/<query>`.

Run: `crypto job run stream_ohlcv [--local] [job args...]`.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    raise NotImplementedError("Task A6 – owner C")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
