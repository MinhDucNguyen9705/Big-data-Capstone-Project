# Owner: C (Speed layer) | Task: A6
"""Trades -> quality + bounded dedupe -> durable handoff -> live candles, latest prices and final gold.

Each stateful query has its own checkpoint: Spark 3.5 cannot chain dedupe and aggregation in update mode.
The handoff Parquet is internal state under checkpoint_root; retain it together with all query checkpoints.
"""

from __future__ import annotations

import argparse
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.common.logging import setup_logging
from cryptopipe.common.spark import get_spark
from cryptopipe.config import Settings
from cryptopipe.jobs.streaming import (
    checkpoint,
    parquet_writer,
    parser,
    preview_sink,
    read_source,
    start_query,
    stop_queries,
    wait_queries,
)
from cryptopipe.serving.cassandra_io import write_latest_price, write_ohlcv
from cryptopipe.symbols import load_symbols
from cryptopipe.transforms.dedupe import dedupe_trades
from cryptopipe.transforms.normalize import parse_trades
from cryptopipe.transforms.ohlcv import ohlcv_1m
from cryptopipe.transforms.quality import validate_trades


def run(spark: SparkSession, settings: Settings, args: argparse.Namespace) -> None:
    raw = read_source(spark, settings, contracts.TOPIC_RAW_TRADES.name, args)
    good, bad = validate_trades(parse_trades(raw), symbols=load_symbols(settings.symbols_file).canonicals)
    unique = dedupe_trades(good, streaming=True, watermark=args.dedupe_watermark)
    handoff = checkpoint(settings, "stream_ohlcv", "trades")
    # Create only the directory. Explicit schema lets downstream readers start before the first batch.
    path = spark._jvm.org.apache.hadoop.fs.Path(handoff)
    path.getFileSystem(spark._jsc.hadoopConfiguration()).mkdirs(path)
    queries = []
    try:
        queries.append(start_query(parquet_writer(unique, handoff), settings, "stream_ohlcv", "dedupe", args))
        # Malformed timestamps must still have an operational partition for quarantine.
        bad = bad.withColumn(
            contracts.PARTITION_DATE,
            F.coalesce(F.col(contracts.PARTITION_DATE), F.date_format(F.current_timestamp(), "yyyy-MM-dd")),
        )
        queries.append(
            start_query(
                parquet_writer(bad, settings.lake_path(contracts.LAKE_QUARANTINE_TRADES), (contracts.PARTITION_DATE,)),
                settings,
                "stream_ohlcv",
                "quarantine",
                args,
            )
        )
        if args.once:
            # AvailableNow snapshots a source at query start: drain producers before starting consumers.
            wait_queries(queries)
        # Consume all committed files together; arbitrary file order must not split an upstream batch
        # and advance the candle watermark ahead of its earlier trades.
        trades = spark.readStream.schema(good.schema).parquet(handoff)
        candles = ohlcv_1m(trades.withWatermark("event_time", args.candle_watermark))
        if args.sink == "cassandra":
            candle_sink = partial(write_ohlcv, settings=settings)
            price_sink = partial(write_latest_price, settings=settings)
        else:
            candle_sink = preview_sink(settings.lake_path("_preview", "ohlcv_updates"))
            price_sink = preview_sink(settings.lake_path("_preview", "latest_price"))
        queries.append(
            start_query(
                candles.writeStream.foreachBatch(candle_sink),
                settings,
                "stream_ohlcv",
                "candles",
                args,
                "update",
            )
        )
        queries.append(
            start_query(
                trades.writeStream.foreachBatch(price_sink),
                settings,
                "stream_ohlcv",
                "latest_price",
                args,
            )
        )
        queries.append(
            start_query(
                parquet_writer(candles, settings.lake_path(contracts.LAKE_GOLD_OHLCV_1M), (contracts.PARTITION_DATE,)),
                settings,
                "stream_ohlcv",
                "gold",
                args,
            )
        )
        wait_queries(queries[2:] if args.once else queries)
    finally:
        stop_queries(queries)


def main(argv: list[str] | None = None) -> int:
    cli = parser("stream_ohlcv", "10 seconds")
    cli.add_argument("--sink", choices=["cassandra", "parquet"], default="cassandra")
    cli.add_argument("--dedupe-watermark", default="2 minutes")
    cli.add_argument("--candle-watermark", default="30 seconds")
    args = cli.parse_args(argv)
    setup_logging()
    spark = get_spark("crypto-stream-ohlcv", local=args.local)
    try:
        run(spark, Settings(), args)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
