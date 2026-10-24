import pytest
import typer
from typer.testing import CliRunner

from cryptopipe.cli import NOT_IMPLEMENTED_EXIT, _run, app

runner = CliRunner()

HELP = [
    [],
    ["ingest"],
    ["ingest", "binance"],
    ["ingest", "coinbase"],
    ["ingest", "coingecko"],
    ["ingest", "fear-greed"],
    ["backfill"],
    ["backfill", "download"],
    ["replay"],
    ["job"],
    ["job", "run"],
    ["job", "list"],
    ["demo"],
    ["demo", "local"],
    ["contracts"],
    ["contracts", "show"],
]


@pytest.mark.parametrize("args", HELP, ids=lambda a: " ".join(a) or "crypto")
def test_every_command_has_help(args):
    result = runner.invoke(app, [*args, "--help"])
    assert result.exit_code == 0, result.output
    assert "Usage" in result.output


def test_contracts_show():
    result = runner.invoke(app, ["contracts", "show"])
    assert result.exit_code == 0, result.output
    for text in ("raw.trades", "dlq.ingest", "bronze/trades", "gold/ohlcv_1m", "ohlcv_1m", "daily_stats"):
        assert text in result.output


def test_stub_maps_to_exit_2_with_owner(capsys):
    # Every bootstrap stub is implemented now; the mapping stays for commands added later.
    def stub():
        raise NotImplementedError("Task X9 – owner Z")

    with pytest.raises(typer.Exit) as exc:
        _run("X9", "Z", stub)
    assert exc.value.exit_code == NOT_IMPLEMENTED_EXIT
    assert "not implemented yet (task X9, owner Z)" in capsys.readouterr().err


def test_replay_dry_run_from_cli(fixtures_dir):
    result = runner.invoke(
        app, ["replay", "--path", str(fixtures_dir / "archive_trades_sample.csv"), "--rate", "1e9", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "replayed 1000 trades" in result.output


def test_batch_job_from_cli(tmp_path):
    result = runner.invoke(
        app, ["job", "run", "profile", "--local", "--lake-root", str(tmp_path), "--raw", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output


def test_unknown_job_and_symbol_rejected():
    assert runner.invoke(app, ["job", "run", "nope"]).exit_code != 0
    assert runner.invoke(app, ["ingest", "binance", "--dry-run", "--symbols", "FOO-USDT"]).exit_code != 0


@pytest.mark.parametrize("name", ["stream_bronze", "stream_ohlcv"])
def test_streaming_invalid_options_fail_before_spark_start(name):
    result = runner.invoke(app, ["job", "run", name, "--no-such-option"])
    assert result.exit_code == 2
    assert "unrecognized arguments" in result.output
    assert "not implemented" not in result.output
