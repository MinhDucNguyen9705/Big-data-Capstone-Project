"""Batch layer gate (owner D): raw archive CSV in the lake -> bronze -> silver -> profile on a real file system.

Run against a fresh, disposable lake root, e.g. on the cluster
`CRYPTO_BATCH_TEST_LAKE_ROOT=hdfs://hdfs-namenode.storage:8020/test/batch-<unique> make test-integration`.
A `file://` root works too (checks the same flow without HDFS). The test refuses an existing root.
"""

import os

import pytest

from cryptopipe import contracts
from cryptopipe.jobs import batch_load_archive, batch_silver, profile

pytestmark = pytest.mark.integration


def test_archive_to_silver_on_the_lake_file_system(spark, fixtures_dir, capsys):
    lake = os.environ.get("CRYPTO_BATCH_TEST_LAKE_ROOT", "").rstrip("/")
    if "://" not in lake:
        pytest.fail(
            "Set CRYPTO_BATCH_TEST_LAKE_ROOT to a fresh lake root, e.g. hdfs://hdfs-namenode.storage:8020/test/x"
        )
    jvm = spark.sparkContext._jvm
    root = jvm.org.apache.hadoop.fs.Path(lake)
    fs = root.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    assert not fs.exists(root), "use a fresh test root so old files cannot hide failures"

    # The download Job's output layout: <lake>/raw/archive/<kind>/<SYMBOL>/<file>.csv
    raw = f"{lake}/{contracts.LAKE_RAW_ARCHIVE}/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.csv"
    fs.copyFromLocalFile(
        jvm.org.apache.hadoop.fs.Path(str(fixtures_dir / "archive_trades_sample.csv")),
        jvm.org.apache.hadoop.fs.Path(raw),
    )

    args = ["--local", "--lake-root", lake]
    assert batch_load_archive.main([*args, "--kind", "trades"]) == 0  # default input: the lake landing zone
    assert batch_load_archive.main([*args, "--kind", "trades"]) == 0  # idempotent rerun
    assert batch_silver.main([*args, "--kind", "trades", "--no-bucketed"]) == 0
    assert profile.main(args) == 0

    bronze = spark.read.parquet(f"{lake}/{contracts.LAKE_BRONZE_ARCHIVE_TRADES}")
    silver = spark.read.parquet(f"{lake}/{contracts.LAKE_SILVER_TRADES}")
    assert bronze.count() == silver.count() == 1000
    assert fs.exists(jvm.org.apache.hadoop.fs.Path(f"{lake}/{contracts.LAKE_SILVER_TRADES}/date=2025-09-01"))
    assert silver.groupBy(*contracts.TRADE_KEY).count().filter("count > 1").count() == 0
    assert fs.exists(jvm.org.apache.hadoop.fs.Path(f"{lake}/{contracts.LAKE_PROFILE}/summary"))
    out = capsys.readouterr().out
    assert "| BTC-USDT | 1 | 1,000 | 2025-09-01 00:00:00.097 | 2025-09-01 00:00:59.118 |" in out
