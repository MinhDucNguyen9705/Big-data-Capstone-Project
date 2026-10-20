"""`crypto` command line entrypoint. Command groups are wired in bootstrap step S7."""

import typer

app = typer.Typer(help="Crypto market data pipeline.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Crypto market data pipeline."""
