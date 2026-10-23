# Owner: D (Batch and data) | Task: A6/A8 | Status: implemented
"""Helpers shared by the batch jobs (`batch_load_archive`, `batch_silver`, `profile`): arguments, the Spark
session and Hadoop FileSystem lookups that work the same on `file://` and `hdfs://`."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cryptopipe.common.spark import to_spark_path


def base_parser(prog: str, description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("--local", action="store_true", help="Local SparkSession (no cluster)")
    parser.add_argument("--lake-root", help="Override CRYPTO_LAKE_ROOT (e.g. a temp dir in tests)")
    return parser


def lake_root(args: argparse.Namespace) -> str:
    from cryptopipe.config import settings

    return to_spark_path(args.lake_root).rstrip("/") if args.lake_root else settings.lake_root


def lake_path(root: str, *parts: str) -> str:
    return "/".join([root, *(p.strip("/") for p in parts)])


def session(app_name: str, local: bool) -> SparkSession:
    from cryptopipe.common.spark import get_spark

    spark = get_spark(app_name, local=local)
    # Reruns must replace only the date partitions they produce (cluster default too, set here because the
    # jobs' idempotence depends on it).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def split_csv_arg(value: str | None) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()] if value else []


@dataclass
class PathSize:
    bytes: int
    files: int


def _fs(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hpath


def path_exists(spark: SparkSession, path: str) -> bool:
    fs, hpath = _fs(spark, path)
    return bool(fs.exists(hpath))


def path_size(spark: SparkSession, path: str) -> PathSize | None:
    """Total bytes and file count under `path` (recursive), None if it does not exist."""
    if not path_exists(spark, path):
        return None
    fs, hpath = _fs(spark, path)
    summary = fs.getContentSummary(hpath)
    return PathSize(int(summary.getLength()), int(summary.getFileCount()))


def read_parquet_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    """Parquet under `path`, or None when the directory is missing or holds no data yet."""
    if not path_exists(spark, path):
        return None
    try:
        return spark.read.parquet(path)
    except Exception as exc:  # e.g. UNABLE_TO_INFER_SCHEMA on an empty streaming sink directory
        if "UNABLE_TO_INFER_SCHEMA" in str(exc) or "Unable to infer schema" in str(exc):
            return None
        raise


def date_filter(dates: list[str]):
    """Filter on a partition-discovered `date` column (DateType) or a string one."""
    return F.col("date").cast("string").isin(*dates)


def human_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"


def markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join("" if v is None else str(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)
