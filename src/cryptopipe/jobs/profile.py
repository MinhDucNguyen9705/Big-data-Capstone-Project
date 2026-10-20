# Owner: D (Batch and data) | Task: A8 | Status: stub, signatures frozen by the bootstrap
"""Data profile of the lake for docs/data.md.

Row counts per symbol/day, min/max timestamps, null counts, duplicates removed, size on disk
(Parquet vs raw CSV). Writes CSVs under `LAKE_PROFILE` and prints a Markdown table.

Run: `crypto job run profile [--local] [job args...]`.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    raise NotImplementedError("Task A8 – owner D")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
