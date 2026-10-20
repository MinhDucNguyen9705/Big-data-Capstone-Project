# Owner: D | Task: A6. Remove the skip marker when implementing cryptopipe.transforms.archive.
import pytest

from cryptopipe.transforms.archive import ARCHIVE_TRADES_SCHEMA, archive_trades_to_contract
from cryptopipe.transforms.normalize import TRADE_COLUMNS

pytestmark = pytest.mark.skip(reason="TODO A6 – owner D")


def test_archive_to_contract_microseconds(spark, fixtures_dir):
    csv = spark.read.csv(str(fixtures_dir / "archive_trades_sample.csv"), schema=ARCHIVE_TRADES_SCHEMA)
    out = archive_trades_to_contract(csv, "BTC-USDT")
    assert set(TRADE_COLUMNS) <= set(out.columns)
    first = out.orderBy("event_ts").first()
    assert first.event_ts == 1_756_684_800_097  # microseconds -> ms
    assert first.exchange == "binance" and first.symbol == "BTC-USDT"
    assert out.count() == 1000
