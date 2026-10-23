# Owner: D | Task: A6. Kline silver transforms: parse, validate, dedupe.
import json

from cryptopipe.transforms.klines import KLINE_COLUMNS, dedupe_klines, parse_klines, validate_klines

GOOD = {
    "exchange": "binance", "symbol": "ETH-USDT", "interval": "1m", "open_ts": 1760000040000,
    "close_ts": 1760000099999, "open": 4510.2, "high": 4512.8, "low": 4508.9, "close": 4511.6,
    "volume": 152.34, "quote_volume": 687289.5, "trades": 1843, "is_closed": True, "schema_version": 1,
}  # fmt: skip


def _values(spark, records):
    return spark.createDataFrame([(r if isinstance(r, str) else json.dumps(r),) for r in records], "value string")


def test_parse_klines_adds_time_columns(spark):
    row = parse_klines(_values(spark, [GOOD])).first()
    assert [row[c] for c in KLINE_COLUMNS] == list(GOOD.values())
    assert (row.date, row.hour) == ("2025-10-09", 8)
    assert row.raw == json.dumps(GOOD)


def _at(minute: int, **changes):
    """GOOD shifted by `minute` minutes (a distinct candle), with `changes` applied."""
    shifted = {**GOOD, "open_ts": GOOD["open_ts"] + minute * 60_000, "close_ts": GOOD["close_ts"] + minute * 60_000}
    return {**shifted, **changes}


def test_validate_klines_reasons(spark):
    records = [
        GOOD,
        _at(1, symbol="FOO-USDT"),
        _at(2, high=4500.0),
        _at(3, low=-1.0),
        _at(4, is_closed=False),
        _at(5, close_ts=GOOD["open_ts"]),
        _at(6, volume=-2.0),
        "{not json",
    ]
    good, bad = validate_klines(parse_klines(_values(spark, records)))
    assert good.count() == 1 and "raw" not in good.columns
    reasons = sorted(r.reason for r in bad.collect())
    assert reasons == sorted(
        [
            "unknown_symbol",
            "inconsistent_ohlc",
            "non_positive_price",
            "not_closed",
            "close_before_open",
            "negative_volume",
            "malformed",
        ]
    )


def test_dedupe_klines_on_business_key(spark):
    again = {**GOOD, "volume": 999.0}  # same exchange, symbol, interval, open_ts
    other = _at(1)
    good, _bad = validate_klines(parse_klines(_values(spark, [GOOD, again, other])))
    assert dedupe_klines(good).count() == 2
