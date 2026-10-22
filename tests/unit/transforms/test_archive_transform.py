# Owner: D | Task: A6. Binance archive CSV -> contract columns -> bronze rows.
import pytest
from pyspark.sql import functions as F

from cryptopipe.contracts import BRONZE_COLUMNS
from cryptopipe.transforms.archive import (
    ARCHIVE_KLINES_SCHEMA,
    ARCHIVE_TRADES_SCHEMA,
    KLINE_COLUMNS,
    archive_klines_to_contract,
    archive_trades_to_contract,
    contract_to_bronze,
    epoch_to_ms,
    with_file_offsets,
)
from cryptopipe.transforms.normalize import TRADE_COLUMNS, TRADE_SCHEMA, parse_trades
from cryptopipe.transforms.quality import validate_trades


@pytest.fixture(scope="module")
def archive_csv(spark, fixtures_dir):
    return spark.read.csv(str(fixtures_dir / "archive_trades_sample.csv"), schema=ARCHIVE_TRADES_SCHEMA)


def test_archive_to_contract_microseconds(spark, fixtures_dir):
    csv = spark.read.csv(str(fixtures_dir / "archive_trades_sample.csv"), schema=ARCHIVE_TRADES_SCHEMA)
    out = archive_trades_to_contract(csv, "BTC-USDT")
    assert set(TRADE_COLUMNS) <= set(out.columns)
    first = out.orderBy("event_ts").first()
    assert first.event_ts == 1_756_684_800_097  # microseconds -> ms
    assert first.exchange == "binance" and first.symbol == "BTC-USDT"
    assert out.count() == 1000


def test_contract_types_match_the_trade_schema(archive_csv):
    out = archive_trades_to_contract(archive_csv, "BTC-USDT")
    assert [(f.name, f.dataType) for f in out.schema.fields] == [(f.name, f.dataType) for f in TRADE_SCHEMA.fields]


def test_epoch_to_ms_detects_units(spark):
    rows = [(1_756_684_800,), (1_756_684_800_097,), (1_756_684_800_097_640,), (1_756_684_800_097_640_123,), (None,)]
    out = spark.createDataFrame(rows, "t long").select(epoch_to_ms("t").alias("ms")).collect()
    assert [r.ms for r in out] == [1_756_684_800_000, *[1_756_684_800_097] * 3, None]


def test_side_and_trade_id(spark):
    rows = [(1, 10.0, 1.0, 10.0, 1_700_000_000_000, True, True), (2, 10.0, 1.0, 10.0, 1_700_000_000_001, False, True)]
    out = archive_trades_to_contract(spark.createDataFrame(rows, ARCHIVE_TRADES_SCHEMA), "ETH-USDT").collect()
    assert [(r.trade_id, r.side, r.event_ts) for r in out] == [
        ("1", "sell", 1_700_000_000_000),
        ("2", "buy", 1_700_000_000_001),
    ]


def test_unknown_symbol_is_rejected(archive_csv):
    with pytest.raises(ValueError, match="FOO-USDT"):
        archive_trades_to_contract(archive_csv, "FOO-USDT")


def test_bronze_rows_round_trip_through_silver_parsing(spark, fixtures_dir):
    csv = spark.read.csv(str(fixtures_dir / "archive_trades_sample.csv"), schema=ARCHIVE_TRADES_SCHEMA)
    contract = archive_trades_to_contract(with_file_offsets(csv, "id"), "BTC-USDT")
    bronze = contract_to_bronze(contract, TRADE_COLUMNS, "event_ts")

    assert bronze.columns == BRONZE_COLUMNS
    meta = bronze.agg(
        F.min("offset").alias("lo"),
        F.max("offset").alias("hi"),
        F.countDistinct("offset").alias("n"),
        F.collect_set("topic").alias("topics"),
        F.collect_set("partition").alias("partitions"),
        F.collect_set("source").alias("sources"),
        F.collect_set("date").alias("dates"),
    ).first()
    assert (meta.lo, meta.hi, meta.n) == (0, 999, 1000)  # row number in the file
    assert (meta.topics, meta.partitions, meta.sources, meta.dates) == (["archive"], [-1], ["archive"], ["2025-09-01"])
    first = bronze.orderBy("offset").first()
    assert first.key == "BTC-USDT" and first.hour == 0

    # silver parses archive bronze exactly like stream bronze
    good, bad = validate_trades(parse_trades(bronze))
    assert bad.count() == 0
    assert (
        good.select(*TRADE_COLUMNS)
        .drop("ingest_ts")
        .subtract(contract.select(*TRADE_COLUMNS).drop("ingest_ts"))
        .count()
        == 0
    )


KLINE_ROWS = [
    # 2025 archive: microseconds
    (1_756_684_800_000_000, 100.0, 105.0, 99.0, 104.0, 3.5, 1_756_684_859_999_999, 360.0, 42, 1.0, 100.0, "0"),
    # pre-2025 archive: milliseconds
    (1_700_000_040_000, 10.0, 11.0, 9.0, 10.5, 1.0, 1_700_000_099_999, 10.0, 7, 0.5, 5.0, "0"),
]


def test_archive_klines_to_contract(spark):
    out = archive_klines_to_contract(spark.createDataFrame(KLINE_ROWS, ARCHIVE_KLINES_SCHEMA), "SOL-USDT")
    assert out.columns == KLINE_COLUMNS
    rows = out.orderBy("open_ts").collect()
    assert [(r.open_ts, r.close_ts) for r in rows] == [
        (1_700_000_040_000, 1_700_000_099_999),
        (1_756_684_800_000, 1_756_684_859_999),
    ]
    assert all(r.is_closed and r.interval == "1m" and r.symbol == "SOL-USDT" for r in rows)
    assert rows[1].trades == 42 and rows[1].quote_volume == 360.0
