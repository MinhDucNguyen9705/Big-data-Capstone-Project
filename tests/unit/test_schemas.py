import pytest
from pydantic import ValidationError
from pyspark.sql.types import ArrayType, DoubleType, LongType, MapType, StringType

from cryptopipe import contracts
from cryptopipe.schemas import (
    Alert,
    AssetMeta,
    DlqRecord,
    FearGreed,
    Kline,
    Trade,
    to_spark_schema,
)
from cryptopipe.symbols import load_symbols

TS = 1_760_000_000_000  # 2025-10-09 UTC, ms

TRADE = dict(
    exchange="binance",
    symbol="BTC-USDT",
    trade_id="5205379727",
    price=62000.5,
    qty=0.01,
    side="buy",
    event_ts=TS,
    ingest_ts=TS + 120,
)

ALL_MODELS = [Trade, Kline, AssetMeta, FearGreed, Alert, DlqRecord]


def test_trade_valid_roundtrip():
    t = Trade(**TRADE)
    assert t.schema_version == 1
    assert Trade.model_validate_json(t.to_json()) == t


def test_trade_rejects_negative_price():
    with pytest.raises(ValidationError):
        Trade(**{**TRADE, "price": -1})


def test_trade_rejects_unknown_symbol():
    with pytest.raises(ValidationError, match="unknown symbol"):
        Trade(**{**TRADE, "symbol": "FOO-USDT"})


def test_trade_rejects_microsecond_timestamp():
    with pytest.raises(ValidationError):
        Trade(**{**TRADE, "event_ts": TS * 1000})


def test_trade_rejects_unknown_field():
    with pytest.raises(ValidationError):
        Trade(**{**TRADE, "extra": 1})


def test_kline_consistency():
    k = dict(
        exchange="binance", symbol="ETH-USDT", interval="1m", open_ts=TS, close_ts=TS + 59_999,
        open=10.0, high=12.0, low=9.0, close=11.0, volume=5, quote_volume=55, trades=3, is_closed=True,
    )  # fmt: skip
    assert Kline(**k).is_closed
    with pytest.raises(ValidationError):
        Kline(**{**k, "high": 10.5})


def test_other_models_parse():
    AssetMeta(coingecko_id="bitcoin", symbol="BTC-USDT", name="Bitcoin", snapshot_ts=TS)
    FearGreed(value=42, classification="Fear", ts=TS)
    Alert(
        alert_id="a1", symbol="SOL-USDT", type="price_spike", severity="warning",
        value=0.07, threshold=0.05, event_ts=TS, details={"window": "1m"},
    )  # fmt: skip
    DlqRecord(source="binance_ws", reason="bad json", raw="{", ingest_ts=TS)
    with pytest.raises(ValidationError):
        FearGreed(value=101, classification="?", ts=TS)


@pytest.mark.parametrize("model", ALL_MODELS, ids=lambda m: m.__name__)
def test_spark_schema_matches_model_fields(model):
    schema = to_spark_schema(model)
    assert schema.fieldNames() == list(model.model_fields)
    assert schema.fieldNames()[-1] == "schema_version"


def test_spark_schema_types():
    trade = {f.name: f for f in to_spark_schema(Trade).fields}
    assert trade["price"].dataType == DoubleType()
    assert trade["event_ts"].dataType == LongType()
    assert trade["side"].dataType == StringType()
    assert not trade["price"].nullable
    meta = {f.name: f for f in to_spark_schema(AssetMeta).fields}
    assert meta["categories"].dataType == ArrayType(StringType(), containsNull=False)
    assert meta["market_cap"].nullable
    alert = {f.name: f for f in to_spark_schema(Alert).fields}
    assert isinstance(alert["details"].dataType, MapType)


def test_symbol_map_lookups():
    sm = load_symbols()
    assert len(sm.canonicals) == 10
    assert sm.to_canonical("binance", "btcusdt") == "BTC-USDT"
    assert sm.to_canonical("coinbase", "ETH-USD") == "ETH-USDT"
    assert sm.to_canonical("coingecko", "avalanche-2") == "AVAX-USDT"
    assert sm.to_canonical("binance", "FOOUSDT") is None
    assert sm.to_exchange("coinbase", "BTC-USDT") == "BTC-USD"
    assert sm.to_exchange("coinbase", "BNB-USDT") is None
    assert "BNBUSDT" in sm.for_exchange("binance")
    assert "BNB-USD" not in sm.for_exchange("coinbase")


def test_contract_topics():
    names = {t.name: t for t in contracts.TOPICS}
    assert names["raw.trades"].partitions == 12
    assert names["raw.klines"].partitions == 6
    assert names["ref.asset_meta"].compacted and names["ref.fear_greed"].compacted
    assert names["alerts"].partitions == 3
    assert names["dlq.ingest"].partitions == 3


def test_cql_defines_contract_tables(fixtures_dir):
    cql = (fixtures_dir.parents[1] / "cql" / "schema.cql").read_text()
    for table in contracts.CASSANDRA_TABLES:
        assert f"crypto.{table} (" in cql
