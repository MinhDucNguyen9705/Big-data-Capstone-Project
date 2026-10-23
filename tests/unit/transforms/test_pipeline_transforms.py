"""Tests for the transforms implemented by the bootstrap (parse, validate, batch dedupe, 1m OHLCV)."""

import json
from datetime import UTC, datetime

import pytest
from chispa import assert_approx_df_equality
from pyspark.sql import functions as F

from cryptopipe.contracts import OHLCV_1M_SCHEMA
from cryptopipe.transforms.dedupe import dedupe_trades
from cryptopipe.transforms.normalize import TRADE_COLUMNS, parse_trades
from cryptopipe.transforms.ohlcv import ohlcv_1m
from cryptopipe.transforms.quality import validate_trades

T = 1_760_054_100_000  # 2025-10-09T23:55:00Z


def trade(**kw):
    base = dict(
        exchange="binance", symbol="BTC-USDT", trade_id="1", price=100.0, qty=1.0, side="buy",
        event_ts=T, ingest_ts=T + 100, schema_version=1,
    )  # fmt: skip
    base.update(kw)
    return json.dumps(base)


def kafka_df(spark, values):
    return spark.createDataFrame([(v,) for v in values], "value string")


def test_parse_trades_types_and_time_columns(spark):
    df = parse_trades(kafka_df(spark, [trade(event_ts=T + 61_000)]))
    assert df.columns == [*TRADE_COLUMNS, "raw", "event_time", "date", "hour"]
    # compare timestamps inside Spark: collected datetimes are converted to the driver's local time zone
    row = df.select("*", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("utc")).first()
    assert row.price == 100.0 and row.event_ts == T + 61_000
    assert row.utc == "2025-10-09 23:56:01"
    assert (row.date, row.hour) == ("2025-10-09", 23)


def test_parse_trades_binary_value_and_malformed(spark):
    df = spark.createDataFrame([(bytearray(trade().encode()),), (bytearray(b"{not json"),)], "value binary")
    rows = parse_trades(df).collect()
    assert rows[0].symbol == "BTC-USDT"
    assert rows[1].symbol is None and rows[1].raw == "{not json"


@pytest.mark.parametrize(
    "value, reason",
    [
        (trade(price=-1.0), "non_positive_price"),
        (trade(qty=0.0), "non_positive_qty"),
        (trade(symbol="FOO-USDT"), "unknown_symbol"),
        (trade(exchange="kraken"), "unknown_exchange"),
        (trade(side="up"), "invalid_side"),
        (trade(event_ts=T + 3_600_000), "future_timestamp"),
        ("{not json", "malformed"),
    ],
)
def test_validate_trades_reasons(spark, value, reason):
    good, bad = validate_trades(parse_trades(kafka_df(spark, [trade(trade_id="ok"), value])))
    assert [r.trade_id for r in good.collect()] == ["ok"]
    assert [r.reason for r in bad.collect()] == [reason]
    assert "raw" not in good.columns and "raw" in bad.columns


def test_dedupe_batch(spark):
    df = parse_trades(kafka_df(spark, [trade(trade_id="1"), trade(trade_id="1"), trade(trade_id="2")]))
    assert dedupe_trades(df).count() == 2
    with pytest.raises(ValueError, match="streaming DataFrame"):
        dedupe_trades(df, streaming=True)


def test_ohlcv_open_close_by_event_time_not_arrival(spark):
    # arrival order: 3rd trade of the minute arrives first (out of order); a duplicate is removed first
    values = [
        trade(trade_id="3", price=103.0, qty=1.0, event_ts=T + 50_000),
        trade(trade_id="1", price=101.0, qty=2.0, event_ts=T + 1_000),
        trade(trade_id="2", price=99.0, qty=1.0, event_ts=T + 20_000),
        trade(trade_id="2", price=99.0, qty=1.0, event_ts=T + 20_000),
        trade(trade_id="4", price=110.0, qty=1.0, event_ts=T + 61_000),
    ]
    good, _ = validate_trades(parse_trades(kafka_df(spark, values)))
    actual = ohlcv_1m(dedupe_trades(good))
    w0, w1, w2 = (datetime(2025, 10, 9, 23, 55 + i, tzinfo=UTC) for i in range(3))
    expected = spark.createDataFrame(
        [
            ("binance", "BTC-USDT", w0, w1, 101.0, 103.0, 99.0, 103.0, 4.0, 404.0, 3, 101.0, "2025-10-09"),
            ("binance", "BTC-USDT", w1, w2, 110.0, 110.0, 110.0, 110.0, 1.0, 110.0, 1, 110.0, "2025-10-09"),
        ],
        OHLCV_1M_SCHEMA,
    )
    assert actual.schema.fieldNames() == OHLCV_1M_SCHEMA.fieldNames()
    assert_approx_df_equality(
        actual.orderBy("window_start"), expected.orderBy("window_start"), 1e-9, ignore_nullable=True
    )


def test_ohlcv_groups_by_exchange_and_symbol(spark):
    values = [trade(), trade(exchange="coinbase"), trade(symbol="ETH-USDT")]
    out = ohlcv_1m(parse_trades(kafka_df(spark, values)))
    assert out.count() == 3
    assert out.agg(F.sum("trades")).first()[0] == 3


@pytest.mark.parametrize(
    "value, reason",
    [
        (trade(price=float("nan")), "non_finite_price"),
        (trade(price=float("inf")), "non_finite_price"),
        (trade(qty=float("inf")), "non_finite_qty"),
        (trade(trade_id="  "), "empty_trade_id"),
        (trade(schema_version=2), "unsupported_schema_version"),
        (trade(unexpected="x"), "unknown_fields"),
        (trade(event_ts=T * 1000), "invalid_timestamp"),
        (trade(event_ts=9_223_372_036_854_775_807), "invalid_timestamp"),
        (trade(ingest_ts=T // 1000), "invalid_timestamp"),
        (trade()[:-1], "malformed"),
        (trade(price="not a number"), "malformed"),
    ],
)
def test_quality_rejects_corrupt_contract_and_timestamp_units(spark, value, reason):
    good, bad = validate_trades(parse_trades(kafka_df(spark, [value])))
    assert good.count() == 0
    row = bad.first()
    assert row.reason == reason
    assert row.raw == value


def test_ohlcv_equal_timestamp_tiebreak_and_zero_volume(spark):
    frame = parse_trades(
        kafka_df(
            spark,
            [
                trade(trade_id="b", price=2.0, qty=0.0),
                trade(trade_id="a", price=1.0, qty=0.0),
            ],
        )
    )
    row = ohlcv_1m(frame).first()
    assert (row.open, row.close, row.volume, row.vwap) == (1.0, 2.0, 0.0, None)


def test_parse_retains_kafka_lineage_for_quarantine(spark):
    raw = kafka_df(spark, [trade(price=-1)]).select(
        "value",
        F.lit("raw.trades").alias("topic"),
        F.lit(2).alias("partition"),
        F.lit(1234).cast("long").alias("offset"),
        F.current_timestamp().alias("timestamp"),
    )
    _, bad = validate_trades(parse_trades(raw))
    row = bad.first()
    assert (row.topic, row.partition, row.offset, row.reason) == ("raw.trades", 2, 1234, "non_positive_price")
    assert row.timestamp is not None
