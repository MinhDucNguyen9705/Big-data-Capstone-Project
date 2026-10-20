# Owner: D | Task: A4. Remove the skip marker when implementing cryptopipe.ingest.archive and replay.
import hashlib
from datetime import date

import pytest

from cryptopipe.ingest import archive, replay

pytestmark = pytest.mark.skip(reason="TODO A4 – owner D")


def test_archive_url_daily_trades():
    url = archive.archive_url("BTCUSDT", "trades", date(2025, 9, 1))
    assert url.endswith("/daily/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.zip")


def test_verify_checksum(tmp_path):
    f = tmp_path / "x.zip"
    f.write_bytes(b"data")
    digest = hashlib.sha256(b"data").hexdigest()
    (tmp_path / "x.zip.CHECKSUM").write_text(f"{digest}  x.zip\n")
    assert archive.verify_checksum(f)
    f.write_bytes(b"tampered")
    assert not archive.verify_checksum(f)


def test_replay_dry_run_counts_rows(fixtures_dir):
    assert replay.replay(fixtures_dir / "archive_trades_sample.csv", None, rate=1e9, dry_run=True) == 1000
