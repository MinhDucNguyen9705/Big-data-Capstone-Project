# Owners: E/C | Task: A6 Cassandra streaming writers.
from datetime import UTC, date, datetime

from cryptopipe.contracts import OHLCV_1M_SCHEMA
from cryptopipe.serving.cassandra_io import ohlcv_to_rows


def test_ohlcv_to_rows_adds_day(spark):
    w = datetime(2025, 10, 9, 23, 59, tzinfo=UTC)
    row = ("binance", "BTC-USDT", w, w, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1, 1.0, "2025-10-09")
    out = ohlcv_to_rows(spark.createDataFrame([row], OHLCV_1M_SCHEMA)).first()
    assert out.day == date(2025, 10, 9)
    assert "updated_at" in out.asDict()


def test_latest_price_uses_event_time_not_arrival_or_highest_price(spark):
    from cryptopipe.serving.cassandra_io import latest_price_to_rows
    from cryptopipe.transforms.normalize import parse_trades
    from tests.unit.transforms.test_pipeline_transforms import T, kafka_df, trade

    frame = parse_trades(
        kafka_df(
            spark,
            [
                trade(trade_id="3", price=99.0, qty=2.0, event_ts=T + 1000),
                trade(trade_id="1", price=999.0, event_ts=T),
                trade(trade_id="2", price=100.0, event_ts=T + 1000),
                trade(exchange="coinbase", trade_id="4", price=88.0, event_ts=T),
            ],
        )
    )
    rows = {r.exchange: r for r in latest_price_to_rows(frame).collect()}
    assert (rows["binance"].price, rows["binance"].qty, rows["binance"].write_ts) == (99, 2, (T + 1000) * 1000)
    assert rows["coinbase"].price == 88


def test_writers_use_configured_table_and_timestamp_option(spark, monkeypatch):
    from cryptopipe.config import Settings
    from cryptopipe.serving import cassandra_io
    from cryptopipe.transforms.normalize import parse_trades
    from tests.unit.transforms.test_pipeline_transforms import kafka_df, trade

    calls = []

    def capture(df, table, settings, **options):
        calls.append((df, table, settings, options))

    monkeypatch.setattr(cassandra_io, "_write", capture)
    settings = Settings(cassandra_hosts="db.example", cassandra_keyspace="test_crypto")
    trades = parse_trades(kafka_df(spark, [trade()]))
    cassandra_io.write_latest_price(trades, 5, settings=settings)
    assert calls[0][1:] == ("latest_price", settings, {"writetime": "write_ts"})
    from cryptopipe.transforms.ohlcv import ohlcv_1m

    cassandra_io.write_ohlcv(ohlcv_1m(trades), 6, settings=settings)
    assert calls[1][1:] == ("ohlcv_1m", settings, {})
    assert set(calls[1][0].columns) == {
        "symbol",
        "day",
        "window_start",
        "exchange",
        "window_end",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades",
        "vwap",
        "updated_at",
    }
