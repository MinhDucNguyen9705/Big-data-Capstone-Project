"""Real Spark file-stream tests: bounded state, recovery, raw lineage and failure propagation."""

from argparse import Namespace
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.config import Settings
from cryptopipe.jobs import stream_bronze, stream_ohlcv
from cryptopipe.jobs.streaming import bronze_records, stop_queries, wait_queries
from cryptopipe.transforms.dedupe import dedupe_trades
from cryptopipe.transforms.normalize import parse_trades
from tests.unit.transforms.test_pipeline_transforms import T, trade


def put(path, name, values):
    # Monotonic modification times keep file-source batches in intended arrival order.
    import os

    path.mkdir(exist_ok=True)
    target = path / f"{name:02}.jsonl"
    target.write_text("\n".join(values) + "\n")
    os.utime(target, (1_800_000_000 + name, 1_800_000_000 + name))


def args(source, **kw):
    values = dict(
        source=str(source),
        once=True,
        trigger="10 seconds",
        max_files_per_trigger=1,
        topic=contracts.TOPIC_RAW_TRADES.name,
        sink="parquet",
        dedupe_watermark="2 minutes",
        candle_watermark="30 seconds",
    )
    return Namespace(**(values | kw))


def test_bronze_retains_binary_raw_and_lineage(spark):
    ts = datetime(2025, 10, 9, 23, 59, tzinfo=UTC)
    frame = spark.createDataFrame(
        [(bytearray(b"BTC-USDT"), bytearray(b"{broken"), "raw.trades", 3, 42, ts)],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )
    out = bronze_records(frame)
    row = out.first()
    assert out.columns == contracts.BRONZE_COLUMNS
    assert (row.key, row.value, row.topic, row.partition, row.offset, row.source) == (
        "BTC-USDT",
        "{broken",
        "raw.trades",
        3,
        42,
        "kafka",
    )
    assert (row.date, row.hour) == ("2025-10-09", 23)


def test_bronze_file_stream_recovery_no_duplicate_files(spark, tmp_path):
    source = tmp_path / "input"
    put(source, 0, [trade(), "{malformed"])
    settings = Settings(lake_root=(tmp_path / "lake").as_uri())
    config = args(source)
    stream_bronze.run(spark, settings, config)
    stream_bronze.run(spark, settings, config)
    bronze = settings.lake_path(contracts.LAKE_BRONZE_TRADES)
    assert spark.read.parquet(bronze).count() == 2
    put(source, 1, [trade(trade_id="2")])
    stream_bronze.run(spark, settings, config)
    rows = spark.read.parquet(bronze).collect()
    assert len(rows) == 3
    assert any(r.value == "{malformed" for r in rows)


def test_dedupe_across_microbatches_watermark_and_restart(spark, tmp_path):
    source = tmp_path / "input"
    put(source, 0, [trade(trade_id="a"), trade(trade_id="a")])
    output, state = str(tmp_path / "output"), str(tmp_path / "state")

    def drain():
        frame = parse_trades(spark.readStream.option("maxFilesPerTrigger", 1).text(str(source)))
        query = (
            dedupe_trades(frame, streaming=True)
            .writeStream.format("parquet")
            .option("path", output)
            .option("checkpointLocation", state)
            .trigger(availableNow=True)
            .start()
        )
        try:
            query.awaitTermination(60)
            assert not query.isActive, "dedupe stream did not drain"
            return query.recentProgress
        finally:
            if query.isActive:
                query.stop()

    drain()
    # Re-delivery after a restart is still inside retained dedupe state.
    put(source, 1, [trade(trade_id="a"), trade(trade_id="b", event_ts=T + 60_000, ingest_ts=T + 60_100)])
    drain()
    # Advance watermark and then supply an unseen trade which is too late.
    put(source, 2, [trade(trade_id="advance", event_ts=T + 600_000, ingest_ts=T + 600_100)])
    drain()
    put(
        source,
        3,
        [
            trade(trade_id="late", event_ts=T + 1000),
            trade(trade_id="fresh", event_ts=T + 601_000, ingest_ts=T + 601_100),
        ],
    )
    progress = drain()
    assert {r.trade_id for r in spark.read.parquet(output).collect()} == {"a", "b", "advance", "fresh"}
    assert sum(op["numRowsDroppedByWatermark"] for p in progress for op in p.get("stateOperators", [])) >= 1


def test_candle_updates_keep_prior_batch_state_and_gold_finalizes(spark, tmp_path):
    source = tmp_path / "input"
    put(source, 0, [trade(trade_id="early", price=100.0, event_ts=T + 1000)])
    settings = Settings(lake_root=(tmp_path / "lake").as_uri())
    config = args(source)
    stream_ohlcv.run(spark, settings, config)
    put(
        source,
        1,
        [
            trade(trade_id="early", price=100.0, event_ts=T + 1000),
            trade(trade_id="later", price=110.0, qty=2.0, event_ts=T + 20_000),
        ],
    )
    stream_ohlcv.run(spark, settings, config)
    updates = spark.read.parquet(settings.lake_path("_preview", "ohlcv_updates"))
    row = updates.orderBy(F.desc("batch_id")).first()
    assert (row.open, row.close, row.low, row.high, row.trades, row.volume) == (100, 110, 100, 110, 2, 3)
    assert row.vwap == pytest.approx(320 / 3)
    put(source, 2, [trade(trade_id="next", event_ts=T + 180_000, ingest_ts=T + 180_100)])
    stream_ohlcv.run(spark, settings, config)
    final = spark.read.parquet(settings.lake_path(contracts.LAKE_GOLD_OHLCV_1M))
    assert final.count() == 1
    assert final.first().trades == 2
    # Unseen late arrival must not reopen a finalized candle after recovering the aggregation state.
    put(
        source,
        3,
        [
            trade(trade_id="too-late", event_ts=T + 2000),
            trade(trade_id="next-2", event_ts=T + 181_000, ingest_ts=T + 181_100),
        ],
    )
    stream_ohlcv.run(spark, settings, config)
    assert spark.read.parquet(settings.lake_path(contracts.LAKE_GOLD_OHLCV_1M)).count() == 1


def test_wait_queries_surfaces_sibling_failure():
    failure = RuntimeError("sink unavailable")
    live = Mock(isActive=True)
    live.recentProgress = []
    live.exception.return_value = None
    failed = Mock(isActive=False)
    failed.exception.return_value = failure
    with pytest.raises(RuntimeError, match="sink unavailable"):
        wait_queries([live, failed])
    stop_queries([live, failed])
    live.stop.assert_called_once()
    failed.stop.assert_not_called()


def test_live_pipeline_consumes_handoff_and_cleans_up_on_failure(spark, tmp_path, monkeypatch):
    """Start all five queries together, then inject a failure after a new file has propagated."""
    source = tmp_path / "input"
    put(source, 0, [trade(trade_id="first")])
    settings = Settings(lake_root=(tmp_path / "lake").as_uri())
    config = args(source, once=False, trigger="100 milliseconds")

    def drive(queries):
        assert len(queries) == 5
        for query in queries:
            query.processAllAvailable()
        put(source, 1, [trade(trade_id="second", price=120.0, event_ts=T + 20_000)])
        # Drain in dependency order: upstream commits first, then each handoff consumer.
        for query in queries:
            query.processAllAvailable()
        raise RuntimeError("controlled interruption")

    monkeypatch.setattr(stream_ohlcv, "wait_queries", drive)
    with pytest.raises(RuntimeError, match="controlled interruption"):
        stream_ohlcv.run(spark, settings, config)
    assert not spark.streams.active
    updates = spark.read.parquet(settings.lake_path("_preview", "ohlcv_updates"))
    row = updates.orderBy(F.desc("batch_id")).first()
    assert (row.trades, row.volume, row.close) == (2, 2.0, 120.0)
