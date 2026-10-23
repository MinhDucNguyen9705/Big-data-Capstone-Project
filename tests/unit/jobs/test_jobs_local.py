# Owners: C (stream_*), D (batch_*, profile) | Task: A6/A8. Remove a skip marker when that job runs locally.
import importlib

import pytest

from cryptopipe.jobs import JOBS


def test_registry_modules_import():
    for module, _owner, _task in JOBS.values():
        assert callable(importlib.import_module(module).main)


def test_batch_silver_local(fixtures_dir, tmp_path):
    from cryptopipe.jobs import batch_silver

    args = ["--local", "--input", str(fixtures_dir / "trades_normalized.jsonl"), "--lake-root", str(tmp_path)]
    assert batch_silver.main(args) == 0


def test_batch_load_archive_local(fixtures_dir, tmp_path):
    from cryptopipe.jobs import batch_load_archive

    args = ["--local", "--input", str(fixtures_dir / "archive_trades_sample.csv"), "--symbol", "BTC-USDT"]
    assert batch_load_archive.main([*args, "--lake-root", str(tmp_path)]) == 0


@pytest.mark.skip(reason="TODO A6 – owner C")
def test_stream_ohlcv_file_source(fixtures_dir):
    """stream_ohlcv over a file stream of the fixture JSONL instead of Kafka (availableNow trigger)."""
    from cryptopipe.jobs import stream_ohlcv

    assert stream_ohlcv.main(["--local", "--source", str(fixtures_dir), "--once"]) == 0
