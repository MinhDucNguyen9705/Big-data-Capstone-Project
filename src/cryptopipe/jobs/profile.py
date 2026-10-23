# Owner: D (Batch and data) | Task: A8 | Status: implemented
"""Data profile of the lake for docs/data.md.

Row counts per symbol/day, min/max timestamps, null counts, duplicates removed, size on disk
(Parquet vs raw CSV). Writes CSVs under `LAKE_PROFILE` and prints a Markdown table.

Run: `crypto job run profile [--local] [job args...]`, e.g.
`crypto job run profile --local --raw ./data/archive` (add `--detail` for the per symbol/day table).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.common.spark import to_spark_path
from cryptopipe.jobs._batch import (
    base_parser,
    human_bytes,
    lake_path,
    lake_root,
    markdown_table,
    path_size,
    read_parquet_if_exists,
    session,
)

# Tables to measure, in lake order: (name, relative path)
STORAGE_TABLES = [
    (name, rel)
    for name, rel in contracts.LAKE_PATHS.items()
    if name.startswith(("bronze_", "silver_", "quarantine_")) or name == "gold_ohlcv_1m"
]
# silver table -> (bronze tables, quarantine table, silver table, event-time column)
SILVER = {
    "trades": (
        [contracts.LAKE_BRONZE_TRADES, contracts.LAKE_BRONZE_ARCHIVE_TRADES], contracts.LAKE_QUARANTINE_TRADES,
        contracts.LAKE_SILVER_TRADES, "event_ts",
    ),
    "klines": (
        [contracts.LAKE_BRONZE_KLINES, contracts.LAKE_BRONZE_ARCHIVE_KLINES], contracts.LAKE_QUARANTINE_KLINES,
        contracts.LAKE_SILVER_KLINES, "open_ts",
    ),
}  # fmt: skip


def _utc(epoch_ms):
    """Epoch ms -> UTC string, formatted by Spark (session time zone UTC): `collect()` would turn timestamps
    into naive datetimes in the driver's local zone."""
    return F.date_format(F.timestamp_millis(epoch_ms), "yyyy-MM-dd HH:mm:ss.SSS")


def by_symbol_day(silver: DataFrame, ts_col: str) -> DataFrame:
    """Rows, first/last event time and null count per symbol and date."""
    nulls = sum((F.col(c).isNull().cast("long") for c in silver.columns if c != contracts.PARTITION_DATE), F.lit(0))
    return (
        silver.groupBy("symbol", F.col(contracts.PARTITION_DATE).cast("string").alias(contracts.PARTITION_DATE))
        .agg(
            F.count(F.lit(1)).alias("rows"),
            _utc(F.min(ts_col)).alias("first_ts"),
            _utc(F.max(ts_col)).alias("last_ts"),
            F.sum(nulls).alias("null_values"),
        )
        .orderBy("symbol", contracts.PARTITION_DATE)
    )


def count_rows(spark: SparkSession, root: str, rel_paths: list[str]) -> int:
    total = 0
    for rel in rel_paths:
        df = read_parquet_if_exists(spark, lake_path(root, rel))
        total += df.count() if df is not None else 0
    return total


def write_csv(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def main(argv: list[str] | None = None) -> int:
    """Entry point. `argv` are the job arguments (without the job name); returns a process exit code."""
    parser = base_parser("profile", "Data profile of the lake (row counts, time ranges, nulls, sizes)")
    parser.add_argument("--raw", default="data/archive", help="Raw archive CSV directory, for the size ratio")
    parser.add_argument("--detail", action="store_true", help="Also print the per symbol/day table")
    args = parser.parse_args(argv)

    root = lake_root(args)
    out = lake_path(root, contracts.LAKE_PROFILE)
    spark = session("profile", args.local)

    # Storage: Parquet size per table, raw CSV size, ratio
    storage = []
    for name, rel in STORAGE_TABLES:
        size = path_size(spark, lake_path(root, rel))
        if size is not None:
            storage.append((name, rel, size.bytes, size.files))
    raw = path_size(spark, to_spark_path(args.raw))
    if raw is not None:
        storage.append(("raw_archive_csv", args.raw, raw.bytes, raw.files))
    sizes = {name: n for name, _rel, n, _files in storage}

    print(f"# Data profile of {root}\n")
    print("## Storage\n")
    print(markdown_table(["table", "path", "size", "files"], [[n, p, human_bytes(b), f] for n, p, b, f in storage]))
    archive_parquet = sum(sizes.get(k, 0) for k in ("bronze_archive_trades", "bronze_archive_klines"))
    if raw is not None and raw.bytes and archive_parquet:
        print(
            f"\nRaw archive CSV {human_bytes(raw.bytes)} -> bronze archive Parquet {human_bytes(archive_parquet)} "
            f"(ratio {archive_parquet / raw.bytes:.2f})"
        )
    if storage:
        write_csv(spark.createDataFrame(storage, "table string, path string, bytes long, files long"), f"{out}/storage")

    # Silver tables: rows in/out, duplicates removed, per symbol and per symbol/day
    summary = []
    for kind, (bronze_rels, quarantine_rel, silver_rel, ts_col) in SILVER.items():
        silver = read_parquet_if_exists(spark, lake_path(root, silver_rel))
        if silver is None:
            continue
        bronze_rows = count_rows(spark, root, bronze_rels)
        quarantined = count_rows(spark, root, [quarantine_rel])
        detail = by_symbol_day(silver, ts_col).cache()
        rows = [r.asDict() for r in detail.collect()]
        silver_rows = sum(r["rows"] for r in rows)
        null_values = sum(r["null_values"] for r in rows)
        dups = bronze_rows - quarantined - silver_rows if bronze_rows else None
        summary.append((kind, bronze_rows, quarantined, dups, silver_rows, null_values, len({r["date"] for r in rows})))
        write_csv(detail, f"{out}/{kind}_by_symbol_day")

        print(f"\n## Silver {kind} per symbol\n")
        per_symbol: dict[str, dict] = {}
        for r in rows:
            s = per_symbol.setdefault(r["symbol"], {"days": 0, "rows": 0, "first": r["first_ts"], "last": r["last_ts"]})
            s["days"] += 1
            s["rows"] += r["rows"]
            s["first"], s["last"] = min(s["first"], r["first_ts"]), max(s["last"], r["last_ts"])
        print(
            markdown_table(
                ["symbol", "days", "rows", "first event (UTC)", "last event (UTC)"],
                [[k, v["days"], f"{v['rows']:,}", v["first"], v["last"]] for k, v in sorted(per_symbol.items())],
            )
        )
        if args.detail:
            print(f"\n## Silver {kind} per symbol and day\n")
            print(
                markdown_table(
                    ["symbol", "date", "rows", "first event", "last event", "nulls"],
                    [
                        [r["symbol"], r["date"], f"{r['rows']:,}", r["first_ts"], r["last_ts"], r["null_values"]]
                        for r in rows
                    ],
                )
            )
        detail.unpersist()

    if not summary:
        print(f"\nNo silver tables under {root} yet (run batch_load_archive and batch_silver first).")
        return 0
    headers = ["table", "bronze rows", "quarantined", "duplicates removed", "silver rows", "null values", "dates"]
    print("\n## Silver summary\n")
    table = [
        [k, f"{b:,}", f"{q:,}", "-" if d is None else f"{d:,}", f"{s:,}", n, nd] for k, b, q, d, s, n, nd in summary
    ]
    print(markdown_table(headers, table))
    schema = "table string, bronze_rows long, quarantined long, duplicates_removed long, silver_rows long, " \
        "null_values long, dates long"  # fmt: skip
    write_csv(spark.createDataFrame(summary, schema), f"{out}/summary")
    print(f"\nCSV reports: {out}/{{storage,summary,<table>_by_symbol_day}}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
