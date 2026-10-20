"""`crypto` command line entrypoint. The remaining command groups are wired in bootstrap step S7."""

from __future__ import annotations

import typer

app = typer.Typer(help="Crypto market data pipeline.", no_args_is_help=True)
demo_app = typer.Typer(help="Runnable demos.", no_args_is_help=True)
app.add_typer(demo_app, name="demo")


@demo_app.command("local")
def demo_local(
    input: str | None = typer.Option(None, help="Trades JSONL (default: tests/fixtures/trades_normalized.jsonl)"),
    lake_root: str | None = typer.Option(None, help="Output lake root (default: CRYPTO_LAKE_ROOT)"),
    allow_remote: bool = typer.Option(False, help="Allow a non-file:// lake root"),
) -> None:
    """Fixture trades -> parse -> validate -> dedupe -> 1m OHLCV in local Spark, written as Parquet."""
    from cryptopipe.demo.local import main

    raise typer.Exit(main(input, lake_root, allow_remote))
