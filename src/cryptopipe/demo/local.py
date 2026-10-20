# Owner: bootstrap (walking skeleton); maintained by C | Status: implemented
"""`crypto demo local`: fixture trades -> parse -> validate -> dedupe -> 1m OHLCV in local Spark -> Parquet.

The fixture JSONL stands in for the Kafka `value` column, so this runs the same transforms as `stream_ohlcv`
and `batch_silver`, with no cluster. Output goes under `CRYPTO_LAKE_ROOT` (local `file://` by default):
bronze/trades, quarantine/trades and gold/ohlcv_1m, partitioned by date.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.common.spark import to_spark_path
from cryptopipe.contracts import PARTITION_DATE, PARTITION_HOUR
from cryptopipe.transforms.dedupe import dedupe_trades
from cryptopipe.transforms.normalize import parse_trades
from cryptopipe.transforms.ohlcv import ohlcv_1m
from cryptopipe.transforms.quality import validate_trades

DEFAULT_INPUT = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "trades_normalized.jsonl"


@dataclass
class DemoResult:
    rows_in: int
    invalid: int
    duplicates_removed: int
    candles_out: int
    candles: DataFrame
    paths: dict[str, str]
    seconds: float


def as_bronze(raw: DataFrame) -> DataFrame:
    """Lines of JSON (column `value`) -> `contracts.BRONZE_COLUMNS`, as if they had been read from Kafka.

    Bronze `date`/`hour` come from the Kafka record timestamp (here: `ingest_ts`), since raw records may not parse.
    """
    ingest_ms = F.get_json_object("value", "$.ingest_ts").cast("long")
    kafka_ts = F.coalesce(F.timestamp_millis(ingest_ms), F.current_timestamp())
    return raw.select(
        F.get_json_object("value", "$.symbol").alias("key"),
        F.col("value"),
        F.lit(contracts.TOPIC_RAW_TRADES.name).alias("topic"),
        F.lit(0).alias("partition"),
        F.monotonically_increasing_id().alias("offset"),
        kafka_ts.alias("kafka_ts"),
        F.lit("demo").alias("source"),
    ).select(
        *contracts.BRONZE_COLUMNS[:-2],
        F.date_format("kafka_ts", "yyyy-MM-dd").alias(PARTITION_DATE),
        F.hour("kafka_ts").alias(PARTITION_HOUR),
    )


def run_demo(spark: SparkSession, input_path: str | Path, lake_root: str) -> DemoResult:
    start = time.perf_counter()
    lake_root = lake_root.rstrip("/")
    paths = {
        "bronze": f"{lake_root}/{contracts.LAKE_BRONZE_TRADES}",
        "quarantine": f"{lake_root}/{contracts.LAKE_QUARANTINE_TRADES}",
        "ohlcv_1m": f"{lake_root}/{contracts.LAKE_GOLD_OHLCV_1M}",
    }

    bronze = as_bronze(spark.read.text(to_spark_path(input_path))).cache()
    trades = parse_trades(bronze).cache()
    good, bad = validate_trades(trades)
    unique = dedupe_trades(good, streaming=False).cache()
    candles = ohlcv_1m(unique).cache()

    rows_in, invalid = bronze.count(), bad.count()
    duplicates_removed = good.count() - unique.count()

    bronze.write.mode("overwrite").partitionBy(PARTITION_DATE, PARTITION_HOUR).parquet(paths["bronze"])
    bad.write.mode("overwrite").partitionBy(PARTITION_DATE).parquet(paths["quarantine"])
    candles.write.mode("overwrite").partitionBy(PARTITION_DATE).parquet(paths["ohlcv_1m"])

    return DemoResult(
        rows_in=rows_in,
        invalid=invalid,
        duplicates_removed=duplicates_removed,
        candles_out=candles.count(),
        candles=candles,
        paths=paths,
        seconds=time.perf_counter() - start,
    )


def print_summary(result: DemoResult) -> None:
    print("crypto demo local: fixture trades -> parse -> validate -> dedupe -> 1m OHLCV")
    print(f"  rows in             {result.rows_in:>6}")
    print(f"  invalid (quarantine){result.invalid:>6}")
    print(f"  duplicates removed  {result.duplicates_removed:>6}")
    print(f"  candles out         {result.candles_out:>6}")
    print(f"  time                {result.seconds:>6.1f} s")
    for name, path in result.paths.items():
        print(f"  {name:<20}{path}")
    print("First 5 candles (UTC):")
    result.candles.orderBy("window_start", "exchange", "symbol").drop("window_end", PARTITION_DATE).show(
        5, truncate=False
    )


def main(input_path: str | Path | None = None, lake_root: str | None = None, allow_remote: bool = False) -> int:
    from cryptopipe.common.spark import get_spark
    from cryptopipe.config import settings

    lake_root = lake_root or settings.lake_root
    if not lake_root.startswith("file:") and not allow_remote:
        print(f"refusing to write demo data to {lake_root}: pass --allow-remote or use a file:// CRYPTO_LAKE_ROOT")
        return 2
    input_path = input_path or DEFAULT_INPUT
    spark = get_spark("crypto-demo-local", local=True)
    print_summary(run_demo(spark, input_path, lake_root))
    return 0
