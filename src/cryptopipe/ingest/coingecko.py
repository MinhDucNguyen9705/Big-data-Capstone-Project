# Owner: B (Ingestion) | Task: A3 | Status: implemented
"""CoinGecko `/coins/markets` poller -> compacted topic `ref.asset_meta` (every 10 minutes, < 10 calls/min).

Raw sample: tests/fixtures/coingecko_markets.json.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx

from cryptopipe.contracts import TOPIC_ASSET_META
from cryptopipe.ingest import kafka_io
from cryptopipe.schemas import AssetMeta
from cryptopipe.schemas.common import now_ms
from cryptopipe.symbols import load_symbols

if TYPE_CHECKING:
    from confluent_kafka import Producer

MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
SOURCE = "coingecko"


def _parse_one(item: dict[str, Any], symbol: str, snapshot_ts: int) -> AssetMeta:
    return AssetMeta(
        coingecko_id=item["id"],
        symbol=symbol,
        name=item["name"],
        market_cap=item.get("market_cap") or None,  # CoinGecko sends 0 when unknown
        circulating_supply=item.get("circulating_supply"),
        snapshot_ts=snapshot_ts,
    )


def _tracked(payload: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
    smap = load_symbols()
    return [(item, s) for item in payload if (s := smap.to_canonical("coingecko", item["id"]))]


def parse_markets(payload: list[dict[str, Any]], snapshot_ts: int) -> list[AssetMeta]:
    """Response of `/coins/markets` -> one `AssetMeta` per tracked asset (untracked ids are skipped)."""
    return [_parse_one(item, symbol, snapshot_ts) for item, symbol in _tracked(payload)]


def _get() -> list[dict[str, Any]]:
    ids = ",".join(load_symbols().for_exchange("coingecko"))
    r = httpx.get(MARKETS_URL, params={"vs_currency": "usd", "ids": ids}, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch() -> list[AssetMeta]:
    """Call the API for every tracked CoinGecko id and parse the response."""
    return parse_markets(_get(), now_ms())


def run_once(producer: Producer | None, dry_run: bool = False) -> int:
    """Fetch and produce to `ref.asset_meta` (key = canonical symbol). Returns the number of messages."""
    snapshot_ts, sent = now_ms(), 0
    for item, symbol in _tracked(_get()):
        try:
            kafka_io.send(producer, TOPIC_ASSET_META, _parse_one(item, symbol, snapshot_ts))
            sent += 1
        except (ValueError, KeyError, TypeError) as e:
            kafka_io.send_dlq(producer, json.dumps(item), f"{type(e).__name__}: {e}", SOURCE)
    if producer is not None:
        producer.flush(30)
    return sent
