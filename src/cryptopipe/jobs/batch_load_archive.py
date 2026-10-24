# Owner: D (Batch and data) | Task: A6/A8 | Status: implemented
"""Archive CSVs -> bronze/archive_trades (and klines) Parquet.

Read headerless CSVs (`transforms.archive.ARCHIVE_TRADES_SCHEMA`), convert with
`archive_trades_to_contract` (us -> ms), write rows with `contracts.BRONZE_COLUMNS` (`source="archive"`) to
`LAKE_BRONZE_ARCHIVE_TRADES` partitioned by date, dynamic partition overwrite (reruns are idempotent).

The symbol of each file comes from `--symbol` or from the file name (`BTCUSDT-trades-2025-09-01.csv`,
`BTCUSDT-1m-2025-09.csv`); files of untracked symbols are skipped. A run replaces every date partition it
writes, so it refuses to drop rows of symbols that are not part of the run unless `--replace-dates` is given.

Run: `crypto job run batch_load_archive [--local] [job args...]`, e.g.
`crypto job run batch_load_archive --local --input ./data/archive/trades`. Without `--input` it reads the lake
landing zone `LAKE_RAW_ARCHIVE/<kind>` (where the cluster download Job puts the CSVs).
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.common.spark import to_spark_path
from cryptopipe.jobs._batch import base_parser, date_filter, lake_path, lake_root, read_parquet_if_exists, session
from cryptopipe.symbols import load_symbols
from cryptopipe.transforms.archive import (
    ARCHIVE_KLINES_SCHEMA,
    ARCHIVE_TRADES_SCHEMA,
    KLINE_COLUMNS,
    SOURCE_FILE,
    TRADE_COLUMNS,
    archive_klines_to_contract,
    archive_trades_to_contract,
    contract_to_bronze,
    with_file_offsets,
)

# Binance symbol in an archive file name: BTCUSDT-trades-2025-09-01.csv, BTCUSDT-1m-2025-09(-01).csv
FILE_SYMBOL_REGEX = r"([A-Z0-9]+)-(?:trades|aggTrades|\d+[smhdwM])-\d{4}-\d{2}(?:-\d{2})?\.csv$"

KINDS = {
    # kind -> (CSV schema, order column, converter, contract columns, event-time column, bronze path)
    "trades": (
        ARCHIVE_TRADES_SCHEMA, "id", archive_trades_to_contract, TRADE_COLUMNS, "event_ts",
        contracts.LAKE_BRONZE_ARCHIVE_TRADES,
    ),
    "klines": (
        ARCHIVE_KLINES_SCHEMA, "open_time", archive_klines_to_contract, KLINE_COLUMNS, "open_ts",
        contracts.LAKE_BRONZE_ARCHIVE_KLINES,
    ),
}  # fmt: skip


def _canonical(symbol: str) -> str:
    symbols = load_symbols()
    if symbol in symbols:
        return symbol
    canonical = symbols.to_canonical("binance", symbol)
    if canonical is None:
        raise ValueError(f"--symbol {symbol!r} is neither a canonical symbol nor a tracked Binance symbol")
    return canonical


def _symbol_from_file():
    """Canonical symbol from the Binance symbol in the file name; null for untracked symbols."""
    pairs = [x for e in load_symbols().entries if e.binance for x in (F.lit(e.binance), F.lit(e.canonical))]
    binance = F.regexp_extract(F.col(SOURCE_FILE), FILE_SYMBOL_REGEX, 1)
    return F.create_map(*pairs)[binance]


def build_parser() -> argparse.ArgumentParser:
    parser = base_parser("batch_load_archive", "Binance archive CSVs -> bronze/archive_* Parquet")
    parser.add_argument("--input", help="CSV file or directory (recursive); default: lake raw/archive/<kind>")
    parser.add_argument("--kind", choices=sorted(KINDS), default="trades")
    parser.add_argument("--symbol", help="Canonical (BTC-USDT) or Binance (BTCUSDT) symbol of every input file")
    parser.add_argument(
        "--replace-dates", action="store_true",
        help="Allow dropping rows of other symbols in the date partitions this run rewrites",
    )  # fmt: skip
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    args = build_parser().parse_args(argv)

    schema, order_col, to_contract, value_cols, ts_col, rel_path = KINDS[args.kind]
    root = lake_root(args)
    input_path = to_spark_path(args.input) if args.input else lake_path(root, contracts.LAKE_RAW_ARCHIVE, args.kind)
    target = lake_path(root, rel_path)
    start = time.perf_counter()

    spark = session(f"batch_load_archive_{args.kind}", args.local)
    try:
        csv = spark.read.csv(input_path, schema=schema, recursiveFileLookup=True, pathGlobFilter="*.csv")
    except Exception as exc:  # AnalysisException: path does not exist
        print(f"batch_load_archive: cannot read {input_path}: {exc}")
        return 1
    # A header line has no numeric id / open time; anything else malformed goes on to silver's quarantine.
    csv = csv.filter(F.col(order_col).isNotNull())
    rows = with_file_offsets(csv, order_col)
    symbol = F.lit(_canonical(args.symbol)) if args.symbol else _symbol_from_file()
    rows = rows.withColumn("_symbol", symbol)

    skipped = rows.filter(F.col("_symbol").isNull()).select(SOURCE_FILE).distinct().collect()
    contract = to_contract(rows.filter(F.col("_symbol").isNotNull()), F.col("_symbol"))
    bronze = contract_to_bronze(contract, value_cols, ts_col).persist()

    stats = bronze.groupBy(contracts.PARTITION_DATE, "key").count().collect()
    if not stats:
        print(f"batch_load_archive: no rows for tracked symbols under {input_path}")
        bronze.unpersist()
        return 1
    dates = sorted({r[contracts.PARTITION_DATE] for r in stats})
    loaded = sorted({r.key for r in stats})

    existing = read_parquet_if_exists(spark, target)
    if existing is not None and not args.replace_dates:
        others = sorted(
            r.key for r in existing.filter(date_filter(dates)).select("key").distinct().collect() if r.key not in loaded
        )
        if others:
            print(
                f"batch_load_archive: refusing to overwrite {len(dates)} date partition(s) of {target} that also hold "
                f"{', '.join(others)}: load those symbols in the same run, or pass --replace-dates"
            )
            bronze.unpersist()
            return 1

    bronze.write.mode("overwrite").partitionBy(contracts.PARTITION_DATE).parquet(target)
    bronze.unpersist()

    total = sum(r["count"] for r in stats)
    print(f"batch_load_archive ({args.kind}): {input_path} -> {target}")
    print(f"  rows written        {total:>12,}")
    print(f"  symbols             {', '.join(loaded)}")
    print(f"  date partitions     {len(dates)} ({dates[0]} .. {dates[-1]})")
    if skipped:
        names = sorted(r[SOURCE_FILE].rsplit("/", 1)[-1] for r in skipped)
        more = " ..." if len(names) > 5 else ""
        print(f"  skipped files       {len(names)} (untracked symbol): {', '.join(names[:5])}{more}")
    print(f"  time                {time.perf_counter() - start:>10.1f} s")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
