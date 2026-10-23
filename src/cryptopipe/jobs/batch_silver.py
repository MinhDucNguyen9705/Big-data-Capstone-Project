# Owner: D (Batch and data) | Task: A6 | Status: implemented
"""Bronze (stream + archive) -> silver trades: typed, validated, deduped.

Union bronze stream and archive, parse_trades -> validate_trades (bad -> quarantine) -> dedupe_trades,
sort within partitions by symbol, event_ts, write `LAKE_SILVER_TRADES` partitioned by date (dynamic overwrite)
and a bucketed catalog table `silver.trades_bucketed` (16 buckets by symbol).

Klines go the same way (`transforms.klines`): bronze/klines + bronze/archive_klines -> `LAKE_SILVER_KLINES`.
`--dates` limits a run to some event dates (the hourly schedule); without it every bronze row is rebuilt.
`--input` replaces the bronze sources with a JSONL file of `Trade` values (tests, laptop runs).

Run: `crypto job run batch_silver [--local] [job args...]`, e.g.
`crypto job run batch_silver --local --input tests/fixtures/trades_normalized.jsonl`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.common.spark import to_spark_path
from cryptopipe.jobs._batch import (
    base_parser,
    date_filter,
    lake_path,
    lake_root,
    path_exists,
    read_parquet_if_exists,
    session,
    split_csv_arg,
)
from cryptopipe.transforms.dedupe import dedupe_trades
from cryptopipe.transforms.klines import KLINE_COLUMNS, dedupe_klines, parse_klines, validate_klines
from cryptopipe.transforms.normalize import TRADE_COLUMNS, parse_trades
from cryptopipe.transforms.quality import validate_trades

BUCKETED_TABLE = "silver.trades_bucketed"
BUCKETS = 16
SILVER_TRADE_COLUMNS = [*TRADE_COLUMNS, "event_time", contracts.PARTITION_DATE]
SILVER_KLINE_COLUMNS = [*KLINE_COLUMNS, "event_time", contracts.PARTITION_DATE]


@dataclass
class SilverResult:
    kind: str
    rows_in: int
    invalid: int
    duplicates_removed: int
    rows_out: int
    dates: list[str]
    path: str


def bronze_dates(dates: list[str]) -> list[str]:
    """Bronze partitions to read for silver `dates`: stream bronze is partitioned by Kafka time, which is at or
    after event time, so a trade of day D can sit in bronze day D+1 (late arrival around midnight)."""
    days = set(dates) | {(date.fromisoformat(d) + timedelta(days=1)).isoformat() for d in dates}
    return sorted(days)


def read_bronze(spark: SparkSession, root: str, rel_paths: list[str], dates: list[str]) -> DataFrame | None:
    """Union of the `value` column of every existing bronze table in `rel_paths` (None if none exists)."""
    frames = []
    for rel in rel_paths:
        df = read_parquet_if_exists(spark, lake_path(root, rel))
        if df is None:
            continue
        if dates:
            df = df.filter(date_filter(bronze_dates(dates)))
        frames.append(df.select("value"))
    return reduce(DataFrame.unionByName, frames) if frames else None


def write_partitioned(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").partitionBy(contracts.PARTITION_DATE).parquet(path)


def write_bucketed(spark: SparkSession, df: DataFrame, table: str = BUCKETED_TABLE, buckets: int = BUCKETS) -> None:
    """Insert `df` into a Parquet table bucketed by symbol (sorted by symbol, event_ts), partitioned by date.

    The table lives at `<spark.sql.warehouse.dir>/<db>.db/<name>`. Without a Hive Metastore (B2) the catalog is
    per session, so the table is re-registered over the existing files and their partitions recovered.
    Dynamic partition overwrite replaces only the dates in `df`.
    """
    db, name = table.split(".")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    if not spark.catalog.tableExists(table):
        location = f"{spark.conf.get('spark.sql.warehouse.dir').rstrip('/')}/{db}.db/{name}"
        columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
        spark.sql(
            f"CREATE TABLE {table} ({columns}) USING parquet PARTITIONED BY ({contracts.PARTITION_DATE}) "
            f"CLUSTERED BY (symbol) SORTED BY (symbol, event_ts) INTO {buckets} BUCKETS LOCATION '{location}'"
        )
        if path_exists(spark, location):
            spark.sql(f"ALTER TABLE {table} RECOVER PARTITIONS")
    df.write.insertInto(table, overwrite=True)


def silver_trades(raw: DataFrame, dates: list[str]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Bronze `value`s -> (all parsed, quarantine, silver), limited to event `dates` if given."""
    trades = parse_trades(raw)
    if dates:
        trades = trades.filter(F.col(contracts.PARTITION_DATE).isin(*dates) | F.col(contracts.PARTITION_DATE).isNull())
    good, bad = validate_trades(trades)
    silver = dedupe_trades(good, streaming=False).select(*SILVER_TRADE_COLUMNS)
    return trades, bad, silver


def silver_klines(raw: DataFrame, dates: list[str]) -> tuple[DataFrame, DataFrame, DataFrame]:
    klines = parse_klines(raw)
    if dates:
        klines = klines.filter(F.col(contracts.PARTITION_DATE).isin(*dates) | F.col(contracts.PARTITION_DATE).isNull())
    good, bad = validate_klines(klines)
    silver = dedupe_klines(good).select(*SILVER_KLINE_COLUMNS)
    return klines, bad, silver


KINDS = {
    # kind -> (bronze values -> (parsed, bad, silver), bronze tables, silver path, quarantine path, sort columns)
    "trades": (
        silver_trades, [contracts.LAKE_BRONZE_TRADES, contracts.LAKE_BRONZE_ARCHIVE_TRADES],
        contracts.LAKE_SILVER_TRADES, contracts.LAKE_QUARANTINE_TRADES, ["symbol", "event_ts", "trade_id"],
    ),
    "klines": (
        silver_klines, [contracts.LAKE_BRONZE_KLINES, contracts.LAKE_BRONZE_ARCHIVE_KLINES],
        contracts.LAKE_SILVER_KLINES, contracts.LAKE_QUARANTINE_KLINES, ["symbol", "open_ts"],
    ),
}  # fmt: skip


def run_kind(
    spark: SparkSession,
    kind: str,
    raw: DataFrame,
    root: str,
    dates: list[str],
    bucketed: bool,
) -> SilverResult:
    to_silver, _bronze, silver_rel, quarantine_rel, sort_cols = KINDS[kind]
    parsed, bad, silver = to_silver(raw, dates)
    # Only silver is cached (it is written twice). `parsed` still carries the raw JSON: caching it would hold
    # the whole input in memory just to count it.
    silver = silver.persist()
    rows_in, invalid, rows_out = parsed.count(), bad.count(), silver.count()
    out_dates = sorted(str(r[0]) for r in silver.select(contracts.PARTITION_DATE).distinct().collect())

    # One task per date, sorted inside: good min/max stats per row group for symbol and time pruning. The sort
    # starts with `date`: partitionBy(date) requires that ordering, and if it is not a prefix Spark (3.4+) adds
    # its own sort by date, which is not stable and scrambles the symbol/time order.
    ordered = silver.repartition(contracts.PARTITION_DATE).sortWithinPartitions(contracts.PARTITION_DATE, *sort_cols)
    silver_path = lake_path(root, silver_rel)
    write_partitioned(ordered, silver_path)
    if invalid:
        write_partitioned(bad, lake_path(root, quarantine_rel))
    if kind == "trades" and bucketed:
        write_bucketed(spark, silver)

    silver.unpersist()
    return SilverResult(kind, rows_in, invalid, rows_in - invalid - rows_out, rows_out, out_dates, silver_path)


def print_result(r: SilverResult) -> None:
    print(f"batch_silver ({r.kind}) -> {r.path}")
    print(f"  rows in             {r.rows_in:>12,}")
    print(f"  invalid (quarantine){r.invalid:>12,}")
    print(f"  duplicates removed  {r.duplicates_removed:>12,}")
    print(f"  rows out            {r.rows_out:>12,}")
    if r.dates:
        print(f"  date partitions     {len(r.dates)} ({r.dates[0]} .. {r.dates[-1]})")


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    parser = base_parser("batch_silver", "Bronze (stream + archive) -> silver trades and klines")
    parser.add_argument("--input", help="JSONL of Trade values instead of the bronze tables (trades only)")
    parser.add_argument("--kind", choices=["trades", "klines", "all"], default="all")
    parser.add_argument("--dates", help="Comma-separated event dates yyyy-MM-dd to rebuild (default: all)")
    parser.add_argument("--no-bucketed", action="store_true", help=f"Skip the {BUCKETED_TABLE} catalog table")
    args = parser.parse_args(argv)

    start = time.perf_counter()
    root = lake_root(args)
    dates = split_csv_arg(args.dates)
    for d in dates:
        date.fromisoformat(d)  # fail fast on a typo
    spark = session("batch_silver", args.local)

    if args.input:
        if args.kind == "klines":
            print("batch_silver: --input carries trades; use the bronze tables for klines")
            return 1
        try:
            sources = {"trades": spark.read.text(to_spark_path(args.input))}
        except Exception as exc:  # AnalysisException: path does not exist
            print(f"batch_silver: cannot read {args.input}: {exc}")
            return 1
    else:
        wanted = list(KINDS) if args.kind == "all" else [args.kind]
        sources = {kind: read_bronze(spark, root, KINDS[kind][1], dates) for kind in wanted}

    for kind, raw in sources.items():
        if raw is None:  # the hourly schedule may run before any bronze exists: not an error
            print(f"batch_silver ({kind}): no bronze input under {root}, nothing to do")
            continue
        print_result(run_kind(spark, kind, raw, root, dates, bucketed=not args.no_bucketed))
    print(f"  time                {time.perf_counter() - start:>10.1f} s")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
