# Owner: B (Ingestion) | Task: A3 | Status: implemented
"""Reconnect loop shared by the WebSocket producers: exponential backoff with jitter, optional proactive reconnect."""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import websockets

from cryptopipe.common.logging import get_logger
from cryptopipe.ingest.kafka_io import RECONNECTS

log = get_logger(__name__)

BACKOFF_MIN_S = 1.0
BACKOFF_MAX_S = 60.0


async def run_forever(
    url: str,
    source: str,
    on_message: Callable[[str | bytes], None],
    *,
    subscribe: Sequence[dict[str, Any]] = (),
    on_connect: Callable[[], Awaitable[None]] | None = None,
    max_age_s: float | None = None,
) -> None:
    """Connect to `url` and feed every message to `on_message` until cancelled.

    `subscribe` messages are sent right after connecting; `on_connect` runs after that (e.g. a REST gap backfill).
    After `max_age_s` the connection is replaced without a backoff; errors and server closes back off.
    """
    backoff = BACKOFF_MIN_S
    while True:
        reason = "closed by server"
        try:
            async with websockets.connect(url, max_size=2**22, open_timeout=15) as ws:
                for sub in subscribe:
                    await ws.send(json.dumps(sub))
                log.info("connected", extra={"source": source})
                if on_connect:
                    await on_connect()
                backoff = BACKOFF_MIN_S
                try:
                    async with asyncio.timeout(max_age_s):
                        async for raw in ws:
                            on_message(raw)
                except TimeoutError:
                    log.info("proactive reconnect", extra={"source": source, "max_age_s": max_age_s})
                    RECONNECTS.labels(source, "proactive").inc()
                    continue
        except Exception as e:  # network, protocol or handshake error: reconnect
            reason = f"{type(e).__name__}: {e}"
        RECONNECTS.labels(source, "error").inc()
        delay = min(backoff + random.uniform(0, backoff / 2), BACKOFF_MAX_S)
        log.warning("disconnected, reconnecting", extra={"source": source, "reason": reason, "delay_s": delay})
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, BACKOFF_MAX_S)
