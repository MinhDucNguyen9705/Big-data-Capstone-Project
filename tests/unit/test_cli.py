import pytest
from typer.testing import CliRunner

from cryptopipe.cli import NOT_IMPLEMENTED_EXIT, app

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


@pytest.mark.parametrize(
    "args, owner",
    [
        (["ingest", "binance", "--dry-run"], "task A3, owner B"),
        (["ingest", "fear-greed", "--dry-run"], "task A3, owner B"),
        (
            ["backfill", "download", "--symbols", "BTCUSDT", "--start", "2025-09-01", "--end", "2025-09-01"],
            "task A4, owner D",
        ),
        (["replay", "--path", "x.csv", "--dry-run"], "task A4, owner D"),
        (["job", "run", "profile"], "task A8, owner D"),
    ],
)
def test_stub_commands_exit_2_with_owner(args, owner):
    result = runner.invoke(app, args)
    assert result.exit_code == NOT_IMPLEMENTED_EXIT
    assert f"not implemented yet ({owner})" in result.output


def test_unknown_job_and_symbol_rejected():
    assert runner.invoke(app, ["job", "run", "nope"]).exit_code != 0
    assert runner.invoke(app, ["ingest", "binance", "--dry-run", "--symbols", "FOO-USDT"]).exit_code != 0


@pytest.mark.parametrize("name", ["stream_bronze", "stream_ohlcv"])
def test_streaming_invalid_options_fail_before_spark_start(name):
    result = runner.invoke(app, ["job", "run", name, "--no-such-option"])
    assert result.exit_code == 2
    assert "unrecognized arguments" in result.output
    assert "not implemented" not in result.output
