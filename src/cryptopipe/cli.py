"""`crypto` command line entrypoint: one command per member's entrypoint.

Commands whose implementation is still a stub exit with code 2 and say which task and owner it belongs to.
Heavy imports (Spark, Kafka) happen inside the commands so `crypto --help` stays fast.
"""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable
from datetime import date
from enum import StrEnum
from typing import Any

import typer

app = typer.Typer(help="Crypto market data pipeline (Kafka, Spark, HDFS, Cassandra).", no_args_is_help=True)
ingest_app = typer.Typer(help="Live producers and pollers (owner B).", no_args_is_help=True)
backfill_app = typer.Typer(help="Historical archive (owner D).", no_args_is_help=True)
job_app = typer.Typer(help="Spark jobs (owners C and D).", no_args_is_help=True)
demo_app = typer.Typer(help="Runnable demos.", no_args_is_help=True)
contracts_app = typer.Typer(help="Frozen interfaces between layers.", no_args_is_help=True)
app.add_typer(ingest_app, name="ingest")
app.add_typer(backfill_app, name="backfill")
app.add_typer(job_app, name="job")
app.add_typer(demo_app, name="demo")
app.add_typer(contracts_app, name="contracts")

NOT_IMPLEMENTED_EXIT = 2


def _run(task: str, owner: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call `fn`; a stub's NotImplementedError becomes a clear message and exit code 2."""
    try:
        return fn(*args, **kwargs)
    except NotImplementedError:
        typer.echo(f"not implemented yet (task {task}, owner {owner})", err=True)
        raise typer.Exit(NOT_IMPLEMENTED_EXIT) from None


def _symbols(symbols: str | None) -> list[str]:
    from cryptopipe.symbols import load_symbols

    tracked = load_symbols().canonicals
    if not symbols:
        return tracked
    chosen = [s.strip() for s in symbols.split(",") if s.strip()]
    unknown = sorted(set(chosen) - set(tracked))
    if unknown:
        raise typer.BadParameter(f"not in the symbols file: {', '.join(unknown)}", param_hint="--symbols")
    return chosen


def _producer(dry_run: bool):
    if dry_run:
        return None
    from cryptopipe.config import settings
    from cryptopipe.ingest.kafka_io import make_producer

    return make_producer(settings)


# --------------------------------------------------------------------------------------------- ingest (B)

SymbolsOpt = typer.Option(None, "--symbols", help="Comma-separated canonical symbols (default: all tracked)")
DryRunOpt = typer.Option(False, "--dry-run", help="Print validated messages instead of producing to Kafka")


def _setup_logging() -> None:
    from cryptopipe.common.logging import setup_logging

    setup_logging()


@ingest_app.command("binance")
def ingest_binance(symbols: str | None = SymbolsOpt, dry_run: bool = DryRunOpt) -> None:
    """Binance WebSocket trades + 1m klines -> raw.trades, raw.klines."""
    from cryptopipe.ingest import binance_ws

    _setup_logging()
    _run("A3", "B", lambda: asyncio.run(binance_ws.run(_symbols(symbols), _producer(dry_run), dry_run=dry_run)))


@ingest_app.command("coinbase")
def ingest_coinbase(symbols: str | None = SymbolsOpt, dry_run: bool = DryRunOpt) -> None:
    """Coinbase WebSocket market_trades -> raw.trades."""
    from cryptopipe.ingest import coinbase_ws

    _setup_logging()
    _run("A3", "B", lambda: asyncio.run(coinbase_ws.run(_symbols(symbols), _producer(dry_run), dry_run=dry_run)))


@ingest_app.command("coingecko")
def ingest_coingecko(dry_run: bool = DryRunOpt) -> None:
    """One CoinGecko /coins/markets poll -> ref.asset_meta (run by a CronJob every 10 min)."""
    from cryptopipe.ingest import coingecko

    _setup_logging()
    _run("A3", "B", lambda: coingecko.run_once(_producer(dry_run), dry_run=dry_run))


@ingest_app.command("fear-greed")
def ingest_fear_greed(dry_run: bool = DryRunOpt) -> None:
    """One Fear & Greed index poll -> ref.fear_greed (run by a daily CronJob)."""
    from cryptopipe.ingest import fear_greed

    _setup_logging()
    _run("A3", "B", lambda: fear_greed.run_once(_producer(dry_run), dry_run=dry_run))


# ------------------------------------------------------------------------------------- backfill / replay (D)


class ArchiveKind(StrEnum):
    trades = "trades"
    klines = "klines"


@backfill_app.command("download")
def backfill_download(
    symbols: str = typer.Option(..., "--symbols", help="Comma-separated Binance symbols, e.g. BTCUSDT,ETHUSDT"),
    kind: ArchiveKind = typer.Option(ArchiveKind.trades, "--kind"),
    start: str = typer.Option(..., "--start", help="First day, YYYY-MM-DD"),
    end: str = typer.Option(..., "--end", help="Last day (inclusive), YYYY-MM-DD"),
    dest: str = typer.Option("./data/archive", "--dest", help="Local directory or hdfs:// path"),
) -> None:
    """Download, verify (SHA256) and unzip Binance archive files from data.binance.vision."""
    from pathlib import Path

    from cryptopipe.ingest import archive

    names = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    files = _run("A4", "D", archive.download, names, kind.value, first, last, Path(dest))
    typer.echo(f"{len(files)} files in {dest}")


@app.command("replay")
def replay_cmd(
    path: str = typer.Option(..., "--path", help="Archive CSV file or directory"),
    speed: float = typer.Option(10.0, "--speed", help="Multiple of real time"),
    rate: float | None = typer.Option(None, "--rate", help="Fixed messages per second (overrides --speed)"),
    rebase_time: bool = typer.Option(False, "--rebase-time", help="Shift event_ts so the first trade is now"),
    dry_run: bool = DryRunOpt,
) -> None:
    """Replay archive trades into raw.trades (owner D)."""
    from pathlib import Path

    from cryptopipe.ingest import replay

    _setup_logging()
    n = _run(
        "A4", "D",
        lambda: replay.replay(Path(path), _producer(dry_run), speed=speed, rate=rate, dry_run=dry_run,
                              rebase_time=rebase_time),
    )  # fmt: skip
    typer.echo(f"replayed {n} trades")


# ------------------------------------------------------------------------------------------------ jobs (C, D)


@job_app.command(
    "run",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def job_run(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Job name, see `crypto job list`"),
    local: bool = typer.Option(False, "--local", help="Run with a local SparkSession (no cluster)"),
) -> None:
    """Run a Spark job: `crypto job run <name> [--local] [job args...]`. Extra args go to the job's main()."""
    from cryptopipe.jobs import JOBS

    if name not in JOBS:
        raise typer.BadParameter(f"unknown job {name!r}; one of: {', '.join(JOBS)}", param_hint="NAME")
    module, owner, task = JOBS[name]
    argv = (["--local"] if local else []) + list(ctx.args)
    code = _run(task, owner, lambda: importlib.import_module(module).main(argv))
    raise typer.Exit(code or 0)


@job_app.command("list")
def job_list() -> None:
    """List the Spark jobs with their owner and task."""
    from cryptopipe.jobs import JOBS

    for name, (module, owner, task) in JOBS.items():
        typer.echo(f"{name:<20} owner {owner}  task {task:<6} {module}")


# ---------------------------------------------------------------------------------------------------- demo


@demo_app.command("local")
def demo_local(
    input: str | None = typer.Option(None, help="Trades JSONL (default: tests/fixtures/trades_normalized.jsonl)"),
    lake_root: str | None = typer.Option(None, help="Output lake root (default: CRYPTO_LAKE_ROOT)"),
    allow_remote: bool = typer.Option(False, help="Allow a non-file:// lake root"),
) -> None:
    """Fixture trades -> parse -> validate -> dedupe -> 1m OHLCV in local Spark, written as Parquet."""
    from cryptopipe.demo.local import main

    raise typer.Exit(main(input, lake_root, allow_remote))


# ----------------------------------------------------------------------------------------------- contracts


@contracts_app.command("show")
def contracts_show() -> None:
    """Print Kafka topics, lake paths, Cassandra tables and the active settings."""
    from cryptopipe import contracts
    from cryptopipe.config import settings
    from cryptopipe.symbols import load_symbols

    typer.echo("Kafka topics")
    typer.echo(f"  {'topic':<16}{'parts':>5}  {'key':<20}{'value':<12}{'retention':<12}producer -> consumers")
    for t in contracts.TOPICS:
        retention = "compacted" if t.compacted else f"{t.retention_ms // contracts.DAY_MS} days"
        typer.echo(
            f"  {t.name:<16}{t.partitions:>5}  {t.key:<20}{t.value_model.__name__:<12}{retention:<12}"
            f"{t.producer} -> {t.consumers}"
        )
    typer.echo(f"\nLake paths (CRYPTO_LAKE_ROOT = {settings.lake_root})")
    for name, rel in contracts.LAKE_PATHS.items():
        typer.echo(f"  {name:<24}{settings.lake_path(rel)}")
    typer.echo(f"  {'checkpoint_root':<24}{settings.checkpoint_root}")
    typer.echo(f"  partition columns       {contracts.PARTITION_DATE}, {contracts.PARTITION_HOUR}")
    typer.echo(f"\nCassandra tables (keyspace {settings.cassandra_keyspace}, hosts {settings.cassandra_hosts})")
    for table, key in contracts.CASSANDRA_TABLES.items():
        typer.echo(f"  {table:<16}PRIMARY KEY {key}")
    symbols = load_symbols()
    typer.echo(f"\nSymbols ({settings.symbols_file}): {', '.join(symbols.canonicals)}")
    typer.echo(f"Kafka bootstrap: {settings.kafka_bootstrap} ({settings.kafka_security_protocol})")


if __name__ == "__main__":
    app()
