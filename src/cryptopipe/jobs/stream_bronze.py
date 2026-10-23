# Owner: C (Speed layer) | Task: A6
"""Kafka raw.trades/raw.klines -> immutable bronze Parquet with independent checkpoints."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from cryptopipe import contracts
from cryptopipe.common.logging import setup_logging
from cryptopipe.common.spark import get_spark
from cryptopipe.config import Settings
from cryptopipe.jobs.streaming import (
    bronze_records,
    parquet_writer,
    parser,
    read_source,
    start_query,
    stop_queries,
    wait_queries,
)


def run(spark: SparkSession, settings: Settings, args: argparse.Namespace) -> None:
    topics = {
        contracts.TOPIC_RAW_TRADES.name: contracts.LAKE_BRONZE_TRADES,
        contracts.TOPIC_RAW_KLINES.name: contracts.LAKE_BRONZE_KLINES,
    }
    selected = [args.topic] if args.topic else list(topics)
    queries = []
    try:
        for topic in selected:
            raw = read_source(spark, settings, topic, args)
            writer = parquet_writer(
                bronze_records(raw),
                settings.lake_path(topics[topic]),
                (contracts.PARTITION_DATE, contracts.PARTITION_HOUR),
            )
            queries.append(start_query(writer, settings, "stream_bronze", topic, args))
        wait_queries(queries)
    finally:
        stop_queries(queries)


def main(argv: list[str] | None = None) -> int:
    cli = parser("stream_bronze", "1 minute")
    cli.add_argument("--topic", choices=[contracts.TOPIC_RAW_TRADES.name, contracts.TOPIC_RAW_KLINES.name])
    args = cli.parse_args(argv)
    if args.source and not args.topic:
        cli.error("--source requires --topic to identify the records in the file stream")
    setup_logging()
    spark = get_spark("crypto-stream-bronze", local=args.local)
    try:
        run(spark, Settings(), args)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
