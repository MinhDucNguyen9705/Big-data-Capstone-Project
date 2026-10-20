# Owner: E | Task: A6. Remove the skip marker when implementing cryptopipe.serving.cassandra_io.
from datetime import UTC, date, datetime

import pytest

from cryptopipe.contracts import OHLCV_1M_SCHEMA
from cryptopipe.serving.cassandra_io import ohlcv_to_rows

pytestmark = pytest.mark.skip(reason="TODO A6 – owner E")


def test_ohlcv_to_rows_adds_day(spark):
    w = datetime(2025, 10, 9, 23, 59, tzinfo=UTC)
    row = ("binance", "BTC-USDT", w, w, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1, 1.0, "2025-10-09")
    out = ohlcv_to_rows(spark.createDataFrame([row], OHLCV_1M_SCHEMA)).first()
    assert out.day == date(2025, 10, 9)
    assert "updated_at" in out.asDict()
