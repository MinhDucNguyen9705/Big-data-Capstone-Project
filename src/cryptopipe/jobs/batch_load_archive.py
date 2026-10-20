# Owner: D (Batch and data) | Task: A6/A8 | Status: stub, signatures frozen by the bootstrap
"""Archive CSVs -> bronze/archive_trades (and klines) Parquet.

Read headerless CSVs (`transforms.archive.ARCHIVE_TRADES_SCHEMA`), convert with
`archive_trades_to_contract` (us -> ms), write rows with `contracts.BRONZE_COLUMNS` (`source="archive"`) to
`LAKE_BRONZE_ARCHIVE_TRADES` partitioned by date, dynamic partition overwrite (reruns are idempotent).

Run: `crypto job run batch_load_archive [--local] [job args...]`.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    raise NotImplementedError("Task A6/A8 – owner D")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
