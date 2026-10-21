"""Real services gate: run only against a disposable test cluster (never a shared Kafka topic).

Requires CRYPTO_SPEED_TEST_LAKE_ROOT=hdfs://.../unique-test-root, Kafka raw topics, a Cassandra keyspace
already initialized from cql/schema.cql and Kafka/Cassandra connector jars in Spark's classpath.
"""

import os
import time
from argparse import Namespace
from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.config import Settings
from cryptopipe.jobs import stream_bronze, stream_ohlcv
from cryptopipe.serving.cassandra_io import get_session, write_latest_price, write_ohlcv
from cryptopipe.transforms.dedupe import dedupe_trades
from cryptopipe.transforms.normalize import parse_trades
from cryptopipe.transforms.ohlcv import ohlcv_1m
from cryptopipe.transforms.quality import validate_trades

pytestmark = pytest.mark.integration


def test_kafka_hdfs_cassandra_and_recovery(spark):
    from confluent_kafka import Producer

    from cryptopipe.schemas import Kline, Trade

    lake = os.environ.get("CRYPTO_SPEED_TEST_LAKE_ROOT", "")
    if not lake.startswith("hdfs://"):
        pytest.fail("Set CRYPTO_SPEED_TEST_LAKE_ROOT to a fresh, disposable HDFS test directory")
    settings = Settings(lake_root=lake, checkpoint_root=f"{lake}/_checkpoints")
    root = spark._jvm.org.apache.hadoop.fs.Path(lake)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    assert not fs.exists(root), "Use a fresh HDFS test root so old sink files cannot hide failures"
    source = Namespace(
        source=None,
        once=True,
        trigger="10 seconds",
        max_files_per_trigger=1,
        starting_offsets="latest",
        max_offsets_per_trigger=1000,
        topic=None,
        sink="cassandra",
        dedupe_watermark="2 minutes",
        candle_watermark="30 seconds",
    )
    session = get_session(settings)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "security.protocol": settings.kafka_security_protocol,
            "acks": "all",
        }
    )
    # Initialize offsets BEFORE publishing: historical contents of a topic must not affect the oracle.
    stream_bronze.run(spark, settings, source)
    stream_ohlcv.run(spark, settings, source)
    source.starting_offsets = "earliest"  # checkpoint offsets must take precedence after recovery
    t0 = int(time.time() * 1000) // 60_000 * 60_000
    unique_id = f"speed-test-{time.time_ns()}"
    values = [
        Trade(
            exchange="binance",
            symbol="BTC-USDT",
            trade_id=f"{unique_id}-{i}",
            price=price,
            qty=qty,
            side="buy",
            event_ts=t0 + delta,
            ingest_ts=t0 + 60_000,
        )
        for i, (delta, price, qty) in enumerate([(1000, 100.0, 1.0), (20_000, 110.0, 2.0), (10_000, 90.0, 1.0)])
    ]
    try:
        for value in [values[1], values[0], values[2], values[1]]:
            producer.produce(contracts.TOPIC_RAW_TRADES.name, value.to_json(), key=value.symbol)
        producer.produce(contracts.TOPIC_RAW_TRADES.name, "{invalid", key="BTC-USDT")
        kline = Kline(
            exchange="binance",
            symbol="BTC-USDT",
            interval="1m",
            open_ts=t0,
            close_ts=t0 + 59_999,
            open=100.0,
            high=110.0,
            low=90.0,
            close=110.0,
            volume=4.0,
            quote_volume=410.0,
            trades=3,
            is_closed=True,
        )
        producer.produce(contracts.TOPIC_RAW_KLINES.name, kline.to_json(), key="BTC-USDT")
        assert producer.flush(30) == 0
        stream_bronze.run(spark, settings, source)
        stream_ohlcv.run(spark, settings, source)
        bronze = spark.read.parquet(settings.lake_path(contracts.LAKE_BRONZE_TRADES))
        good, _ = validate_trades(parse_trades(bronze))
        expected = ohlcv_1m(dedupe_trades(good)).filter(F.unix_millis("window_start") == t0).first()
        assert expected.trades == 3
        query = session.prepare(
            f"SELECT * FROM {settings.cassandra_keyspace}.{contracts.CASSANDRA_OHLCV_1M} "
            "WHERE symbol=? AND day=? AND window_start=? AND exchange=?"
        )
        when = datetime.fromtimestamp(t0 / 1000, tz=UTC)
        key = ("BTC-USDT", when.date(), when, "binance")
        row = session.execute(query, key).one()
        assert row is not None
        for name in ("open", "high", "low", "close", "volume", "quote_volume", "vwap", "trades"):
            assert getattr(row, name) == pytest.approx(expected[name])
        # Replay the exact output batch to exercise the actual connector upsert, then recover both jobs.
        write_ohlcv(ohlcv_1m(dedupe_trades(good)), 0, settings=settings)
        stream_bronze.run(spark, settings, source)
        stream_ohlcv.run(spark, settings, source)
        assert spark.read.parquet(settings.lake_path(contracts.LAKE_BRONZE_TRADES)).count() == 5
        assert spark.read.parquet(settings.lake_path(contracts.LAKE_BRONZE_KLINES)).count() == 1
        assert session.execute(query, key).one().trades == 3
        assert spark.read.parquet(settings.lake_path(contracts.LAKE_QUARANTINE_TRADES)).count() == 1
        # An old write through the connector must not overwrite the latest price from the stream.
        write_latest_price(
            parse_trades(spark.createDataFrame([(values[0].to_json(),)], "value string")), 999, settings=settings
        )
        latest = session.execute(
            f"SELECT * FROM {settings.cassandra_keyspace}.{contracts.CASSANDRA_LATEST_PRICE} "
            "WHERE symbol='BTC-USDT' AND exchange='binance'"
        ).one()
        assert latest.price == 110.0
        assert latest.qty == 2.0
        advance = values[0].model_copy(
            update={
                "trade_id": f"{unique_id}-advance",
                "event_ts": t0 + 180_000,
                "ingest_ts": t0 + 180_001,
            }
        )
        producer.produce(contracts.TOPIC_RAW_TRADES.name, advance.to_json(), key=advance.symbol)
        assert producer.flush(30) == 0
        stream_ohlcv.run(spark, settings, source)
        gold = spark.read.parquet(settings.lake_path(contracts.LAKE_GOLD_OHLCV_1M))
        finalized = gold.filter(F.unix_millis("window_start") == t0)
        assert finalized.count() == 1 and finalized.first().trades == 3
    finally:
        session.shutdown()
        session.cluster.shutdown()
