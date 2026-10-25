"""Walking skeleton: the demo pipeline reproduces expected_ohlcv_1m.json from trades_normalized.jsonl."""

import json
from datetime import datetime

from chispa import assert_approx_df_equality

from cryptopipe.common.spark import to_spark_path
from cryptopipe.contracts import OHLCV_1M_SCHEMA
from cryptopipe.demo.local import run_demo


def _expected(spark, fixtures_dir):
    rows = json.loads((fixtures_dir / "expected_ohlcv_1m.json").read_text())
    for r in rows:
        for col in ("window_start", "window_end"):
            r[col] = datetime.fromisoformat(r[col])  # "...Z" -> aware UTC datetime
    return spark.createDataFrame(rows, OHLCV_1M_SCHEMA)


def test_demo_local_matches_expected_candles(spark, fixtures_dir, tmp_path):
    lake = to_spark_path(tmp_path)
    result = run_demo(spark, fixtures_dir / "trades_normalized.jsonl", lake)

    assert (result.rows_in, result.invalid, result.duplicates_removed) == (500, 3, 5)
    assert result.candles_out == 60
    order = ["window_start", "exchange", "symbol"]
    assert_approx_df_equality(
        result.candles.orderBy(order),
        _expected(spark, fixtures_dir).orderBy(order),
        1e-6,
        ignore_nullable=True,
    )

    written = spark.read.parquet(result.paths["ohlcv_1m"])
    assert written.count() == 60
    # partition discovery reads `date` back as DateType
    assert sorted(str(r.date) for r in written.select("date").distinct().collect()) == ["2025-10-09", "2025-10-10"]
    reasons = {r.reason for r in spark.read.parquet(result.paths["quarantine"]).collect()}
    assert reasons == {"non_positive_price", "unknown_symbol", "future_timestamp"}
    assert spark.read.parquet(result.paths["bronze"]).count() == 500
