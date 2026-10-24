# Owner: D | Task: A6/A8. Batch layer end to end in local Spark: archive CSV -> bronze -> silver -> profile.
import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from cryptopipe import contracts
from cryptopipe.common.spark import to_spark_path
from cryptopipe.demo.local import as_bronze
from cryptopipe.jobs import batch_load_archive, batch_silver, profile

ARCHIVE_FIRST_TS = 1_756_684_800_097


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "lake"


@pytest.fixture
def raw_trades(tmp_path, fixtures_dir):
    """The fixture CSV in the downloader layout, so the job takes the symbol from the file name."""
    folder = tmp_path / "raw" / "trades" / "BTCUSDT"
    folder.mkdir(parents=True)
    shutil.copy(fixtures_dir / "archive_trades_sample.csv", folder / "BTCUSDT-trades-2025-09-01.csv")
    return folder.parent


@pytest.fixture
def fresh_bucketed(spark):
    """The bucketed table lives in the session warehouse, shared by all tests: start each test without it."""

    def drop():
        if spark.catalog.tableExists(batch_silver.BUCKETED_TABLE):
            location = (
                spark.sql(f"DESCRIBE TABLE EXTENDED {batch_silver.BUCKETED_TABLE}")
                .filter("col_name = 'Location'")
                .first()
                .data_type
            )
            spark.sql(f"DROP TABLE {batch_silver.BUCKETED_TABLE}")
            shutil.rmtree(location.removeprefix("file:"), ignore_errors=True)

    drop()
    yield
    drop()


def _read(spark, lake: Path, rel: str):
    return spark.read.parquet(to_spark_path(lake / rel))


def _load(*args: str) -> int:
    return batch_load_archive.main(["--local", *args])


def _silver(*args: str) -> int:
    return batch_silver.main(["--local", *args])


def test_load_archive_writes_bronze_and_is_idempotent(spark, lake, raw_trades):
    assert _load("--input", str(raw_trades), "--lake-root", str(lake)) == 0
    assert _load("--input", str(raw_trades), "--lake-root", str(lake)) == 0  # rerun replaces, never appends

    bronze = _read(spark, lake, contracts.LAKE_BRONZE_ARCHIVE_TRADES)
    assert set(bronze.columns) == set(contracts.BRONZE_COLUMNS)  # partition column `date` is read back last
    assert bronze.count() == 1000
    assert [str(r.date) for r in bronze.select("date").distinct().collect()] == ["2025-09-01"]
    assert {r.key for r in bronze.select("key").distinct().collect()} == {"BTC-USDT"}
    first = json.loads(bronze.orderBy("offset").first().value)
    assert first["event_ts"] == ARCHIVE_FIRST_TS and first["trade_id"] == "5205379727"


def test_load_archive_refuses_to_drop_other_symbols(spark, lake, raw_trades):
    assert _load("--input", str(raw_trades), "--lake-root", str(lake)) == 0
    # the same day loaded as another symbol would replace the BTC rows of date=2025-09-01
    assert _load("--input", str(raw_trades), "--symbol", "ETHUSDT", "--lake-root", str(lake)) == 1
    assert {r.key for r in _read(spark, lake, contracts.LAKE_BRONZE_ARCHIVE_TRADES).collect()} == {"BTC-USDT"}

    assert _load("--input", str(raw_trades), "--symbol", "ETH-USDT", "--replace-dates", "--lake-root", str(lake)) == 0
    assert {r.key for r in _read(spark, lake, contracts.LAKE_BRONZE_ARCHIVE_TRADES).collect()} == {"ETH-USDT"}


def test_load_archive_skips_untracked_symbols_and_missing_input(lake, tmp_path, fixtures_dir, capsys):
    folder = tmp_path / "raw" / "trades" / "PEPEUSDT"
    folder.mkdir(parents=True)
    shutil.copy(fixtures_dir / "archive_trades_sample.csv", folder / "PEPEUSDT-trades-2025-09-01.csv")
    assert _load("--input", str(folder), "--lake-root", str(lake)) == 1
    assert "no rows for tracked symbols" in capsys.readouterr().out
    assert _load("--input", str(tmp_path / "absent"), "--lake-root", str(lake)) == 1


def test_silver_merges_stream_and_archive_bronze(spark, lake, raw_trades, fixtures_dir, fresh_bucketed, capsys):
    assert _load("--input", str(raw_trades), "--lake-root", str(lake)) == 0
    # Stream bronze: the 500 fixture trades plus the first 100 archive trades again, as the live producer would
    # have sent them (same exchange, symbol, trade_id -> must be deduplicated against the archive).
    archive_values = _read(spark, lake, contracts.LAKE_BRONZE_ARCHIVE_TRADES).orderBy("offset").limit(100)
    stream_lines = spark.read.text(str(fixtures_dir / "trades_normalized.jsonl")).unionByName(
        archive_values.select(F.col("value"))
    )
    as_bronze(stream_lines).write.partitionBy("date", "hour").parquet(
        to_spark_path(lake / contracts.LAKE_BRONZE_TRADES)
    )

    for _ in range(2):  # idempotent rerun
        assert _silver("--kind", "trades", "--lake-root", str(lake)) == 0

    silver = _read(spark, lake, contracts.LAKE_SILVER_TRADES)
    assert silver.count() == 1000 + 492  # 500 fixture rows - 3 invalid - 5 duplicates; 100 overlaps removed
    assert silver.groupBy(*contracts.TRADE_KEY).count().filter("count > 1").count() == 0
    assert set(silver.columns) == set(batch_silver.SILVER_TRADE_COLUMNS)
    assert sorted(str(r.date) for r in silver.select("date").distinct().collect()) == [
        "2025-09-01",
        "2025-10-09",
        "2025-10-10",
    ]
    # The 3 invalid rows came from stream bronze: stream_ohlcv owns their quarantine (a streaming file sink), so
    # batch_silver only counts them and writes nothing into either quarantine folder.
    assert not (lake / contracts.LAKE_QUARANTINE_TRADES).exists()
    assert not (lake / contracts.LAKE_QUARANTINE_ARCHIVE_TRADES).exists()
    assert "of which stream              3" in capsys.readouterr().out

    table = spark.table(batch_silver.BUCKETED_TABLE)
    assert table.count() == 1492
    detail = {
        r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {batch_silver.BUCKETED_TABLE}").collect()
    }
    assert detail["Num Buckets"] == "16"
    assert detail["Bucket Columns"] == "[`symbol`]"
    assert detail["Sort Columns"] == "[`symbol`, `event_ts`]"


def test_silver_files_are_sorted_by_symbol_and_time(spark, lake, fixtures_dir):
    jsonl = str(fixtures_dir / "trades_normalized.jsonl")
    assert _silver("--input", jsonl, "--lake-root", str(lake), "--no-bucketed") == 0
    day = spark.read.parquet(to_spark_path(lake / contracts.LAKE_SILVER_TRADES / "date=2025-10-09"))
    assert len(day.inputFiles()) == 1
    rows = [(r.symbol, r.event_ts) for r in day.select("symbol", "event_ts").collect()]  # file order
    assert rows == sorted(rows)


def test_silver_dates_rebuild_only_those_dates(spark, lake, fixtures_dir, fresh_bucketed):
    jsonl = str(fixtures_dir / "trades_normalized.jsonl")
    assert _silver("--input", jsonl, "--lake-root", str(lake), "--dates", "2025-10-10") == 0
    silver = _read(spark, lake, contracts.LAKE_SILVER_TRADES)
    assert [str(r.date) for r in silver.select("date").distinct().collect()] == ["2025-10-10"]
    assert _silver("--input", jsonl, "--lake-root", str(lake), "--dates", "2025-10-09") == 0
    assert _read(spark, lake, contracts.LAKE_SILVER_TRADES).count() == 492  # both dates, nothing duplicated
    with pytest.raises(ValueError):
        _silver("--input", jsonl, "--lake-root", str(lake), "--dates", "2025-13-01")


def test_archive_rejects_go_to_archive_quarantine(spark, lake, tmp_path, fresh_bucketed):
    folder = tmp_path / "raw" / "trades" / "ETHUSDT"
    folder.mkdir(parents=True)
    rows = ["1,4000.0,1.0,4000.0,1756684800000000,True,True", "2,-1.0,1.0,-1.0,1756684801000000,False,True"]
    (folder / "ETHUSDT-trades-2025-09-01.csv").write_text("\n".join(rows) + "\n")
    assert _load("--input", str(folder.parent), "--lake-root", str(lake)) == 0
    assert _silver("--kind", "trades", "--lake-root", str(lake)) == 0

    bad = _read(spark, lake, contracts.LAKE_QUARANTINE_ARCHIVE_TRADES).collect()
    assert [(r.trade_id, r.reason, r.topic) for r in bad] == [("2", "non_positive_price", "archive")]
    assert _read(spark, lake, contracts.LAKE_SILVER_TRADES).count() == 1


def test_silver_never_writes_into_a_streaming_sink(lake, fixtures_dir):
    sink = lake / contracts.LAKE_QUARANTINE_ARCHIVE_TRADES / "_spark_metadata"
    sink.mkdir(parents=True)  # as if a streaming query owned the folder
    jsonl = str(fixtures_dir / "trades_normalized.jsonl")
    with pytest.raises(RuntimeError, match="streaming file sink"):
        _silver("--input", jsonl, "--lake-root", str(lake), "--no-bucketed")


def test_recent_days_window():
    from datetime import date

    assert batch_silver.recent_days(2, today=date(2025, 3, 1)) == ["2025-02-28", "2025-03-01"]
    args = batch_silver.build_parser().parse_args(["--recent-days", "2"])
    assert args.recent_days == 2 and args.dates is None
    with pytest.raises(SystemExit):  # --dates and --recent-days are exclusive
        batch_silver.build_parser().parse_args(["--recent-days", "2", "--dates", "2025-09-01"])


def test_silver_without_bronze_is_a_no_op(lake, capsys):
    assert _silver("--lake-root", str(lake)) == 0
    assert "nothing to do" in capsys.readouterr().out


KLINES_CSV = [
    # open_time (us), open, high, low, close, volume, close_time, quote_volume, trades, taker base, taker quote, ignore
    "1756684800000000,100.0,105.0,99.0,104.0,3.5,1756684859999999,360.0,42,1.0,100.0,0",
    "1756684860000000,104.0,106.0,103.0,105.0,2.0,1756684919999999,210.0,30,1.0,105.0,0",
    "1756684920000000,105.0,104.0,103.0,105.5,2.0,1756684979999999,210.0,30,1.0,105.0,0",  # high < close
]


def test_klines_archive_to_silver(spark, lake, tmp_path):
    folder = tmp_path / "raw" / "klines" / "SOLUSDT"
    folder.mkdir(parents=True)
    (folder / "SOLUSDT-1m-2025-09.csv").write_text("\n".join(KLINES_CSV) + "\n")

    assert _load("--kind", "klines", "--input", str(folder.parent), "--lake-root", str(lake)) == 0
    assert _silver("--kind", "klines", "--lake-root", str(lake)) == 0

    assert _read(spark, lake, contracts.LAKE_BRONZE_ARCHIVE_KLINES).count() == 3
    silver = _read(spark, lake, contracts.LAKE_SILVER_KLINES).orderBy("open_ts").collect()
    assert [(r.symbol, r.open_ts, r.trades) for r in silver] == [
        ("SOL-USDT", 1_756_684_800_000, 42),
        ("SOL-USDT", 1_756_684_860_000, 30),
    ]
    assert [r.reason for r in _read(spark, lake, contracts.LAKE_QUARANTINE_KLINES).collect()] == ["inconsistent_ohlc"]


def test_profile_reports_counts_and_sizes(spark, lake, raw_trades, fresh_bucketed, capsys):
    assert _load("--input", str(raw_trades), "--lake-root", str(lake)) == 0
    assert _silver("--kind", "trades", "--lake-root", str(lake)) == 0
    capsys.readouterr()

    assert profile.main(["--local", "--lake-root", str(lake), "--raw", str(raw_trades), "--detail"]) == 0

    out = capsys.readouterr().out
    assert "## Storage" in out and "raw_archive_csv" in out and "ratio" in out
    assert "| BTC-USDT | 1 | 1,000 | 2025-09-01 00:00:00.097 | 2025-09-01 00:00:59.118 |" in out
    assert "| trades | 1,000 | 0 | 0 | 1,000 | 0 | 1 |" in out
    reports = lake / contracts.LAKE_PROFILE
    summary = spark.read.option("header", True).csv(to_spark_path(reports / "summary")).first()
    assert (summary.table, summary.silver_rows, summary.duplicates_removed) == ("trades", "1000", "0")
    assert spark.read.option("header", True).csv(to_spark_path(reports / "trades_by_symbol_day")).count() == 1
    assert spark.read.option("header", True).csv(to_spark_path(reports / "storage")).count() >= 3


def test_profile_on_an_empty_lake(lake, capsys):
    assert profile.main(["--local", "--lake-root", str(lake), "--raw", str(lake / "none")]) == 0
    assert "No silver tables" in capsys.readouterr().out
