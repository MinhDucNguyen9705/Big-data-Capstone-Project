# Owner: C (Speed layer) | Task: A6 | Status: stub, signatures frozen by the bootstrap
"""Kafka `raw.trades` + `raw.klines` -> bronze Parquet (append, exactly-once file sink).

Read both topics with the Kafka source, keep `key`, raw `value`, `topic`, `partition`, `offset`, `timestamp`
(as `kafka_ts`), add `source="kafka"`, `date`/`hour` from `kafka_ts`, and write Parquet to
`settings.lake_path(LAKE_BRONZE_TRADES / LAKE_BRONZE_KLINES)` partitioned by date, hour; trigger 1 minute;
checkpoint `{settings.checkpoint_root}/stream_bronze/<topic>`.

Run: `crypto job run stream_bronze [--local] [job args...]`.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    raise NotImplementedError("Task A6 – owner C")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
