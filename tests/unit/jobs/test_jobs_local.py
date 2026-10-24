# Owners: C (stream_*), D (batch_*, profile) | Task: A6/A8. Remove a skip marker when that job runs locally.
import importlib

from cryptopipe.jobs import JOBS


def test_registry_modules_import():
    for module, _owner, _task in JOBS.values():
        assert callable(importlib.import_module(module).main)


def test_batch_silver_local(fixtures_dir, tmp_path):
    from cryptopipe.jobs import batch_silver

    args = ["--local", "--input", str(fixtures_dir / "trades_normalized.jsonl"), "--lake-root", str(tmp_path)]
    assert batch_silver.main(args) == 0


def test_batch_load_archive_local(fixtures_dir, tmp_path):
    from cryptopipe.jobs import batch_load_archive

    args = ["--local", "--input", str(fixtures_dir / "archive_trades_sample.csv"), "--symbol", "BTC-USDT"]
    assert batch_load_archive.main([*args, "--lake-root", str(tmp_path)]) == 0


def test_stream_ohlcv_file_source(spark, fixtures_dir, tmp_path):
    """stream_ohlcv over a file stream of the fixture JSONL instead of Kafka (availableNow trigger)."""
    from argparse import Namespace

    from chispa import assert_approx_df_equality
    from pyspark.sql import functions as F

    from cryptopipe.config import Settings
    from cryptopipe.jobs import stream_ohlcv
    from cryptopipe.transforms.dedupe import dedupe_trades
    from cryptopipe.transforms.normalize import parse_trades
    from cryptopipe.transforms.ohlcv import ohlcv_1m
    from cryptopipe.transforms.quality import validate_trades

    path = str(fixtures_dir / "trades_normalized.jsonl")
    settings = Settings(lake_root=tmp_path.as_uri())
    args = Namespace(
        source=path,
        once=True,
        trigger="10 seconds",
        max_files_per_trigger=1,
        dedupe_watermark="2 minutes",
        candle_watermark="30 seconds",
        sink="parquet",
    )
    stream_ohlcv.run(spark, settings, args)
    updates = spark.read.parquet(settings.lake_path("_preview", "ohlcv_updates"))
    # Select the latest update per key; multiple batches may have touched a candle.
    latest = (
        updates.groupBy("symbol", "exchange", "window_start")
        .agg(F.max_by(F.struct(*[F.col(c) for c in updates.columns if c != "batch_id"]), "batch_id").alias("c"))
        .select("c.*")
    )
    good, _ = validate_trades(parse_trades(spark.read.text(path)))
    expected = ohlcv_1m(dedupe_trades(good))
    order = ["symbol", "exchange", "window_start"]
    assert_approx_df_equality(
        latest.select(*expected.columns).orderBy(*order),
        expected.orderBy(*order),
        1e-6,
        ignore_nullable=True,
    )
    assert spark.read.parquet(settings.lake_path("quarantine/trades")).count() == 3
    gold = spark.read.parquet(settings.lake_path("gold/ohlcv_1m"))
    assert gold.count() > 0
    assert gold.groupBy(*order).count().filter("count > 1").count() == 0
    before = spark.read.parquet(settings.lake_path("_preview", "ohlcv_updates")).count()
    stream_ohlcv.run(spark, settings, args)
    assert spark.read.parquet(settings.lake_path("_preview", "ohlcv_updates")).count() == before
