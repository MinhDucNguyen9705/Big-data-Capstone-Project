"""Owner E: real Cassandra gate for the exact dashboard CQL, isolated in crypto_demo."""

from __future__ import annotations

import os
import runpy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cryptopipe.serving.check import interpolate
from cryptopipe.serving.seed_demo import render

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_cql_same_day_and_utc_midnight_on_real_cassandra():
    host = os.environ.get("CRYPTO_SERVING_TEST_HOST")
    if not host:
        pytest.skip("Set CRYPTO_SERVING_TEST_HOST to an isolated Cassandra; writes crypto_demo only")
    from cassandra.cluster import Cluster

    generator = runpy.run_path(str(ROOT / "deploy/monitoring/generate.py"))
    cluster = Cluster([host], port=int(os.environ.get("CRYPTO_SERVING_TEST_PORT", "9042")))
    session = cluster.connect()
    try:
        end = datetime(2026, 10, 24, 0, 1, tzinfo=UTC)
        cql = render((ROOT / "cql/schema.cql").read_text(), end, minutes=2)
        # Statements in the canonical schema are not allowed to escape the demo keyspace.
        assert "INSERT INTO crypto." not in cql
        # Schema comments contain semicolons; remove comments before splitting
        # these known generated statements (production loading uses cqlsh -f).
        statements = "\n".join(line.split("--", 1)[0] for line in cql.splitlines()).split(";")
        for _ in range(2):  # schema and candle writes must be safe to repeat
            for statement in statements:
                if statement.strip():
                    session.execute(statement)
        candle_query = generator["candle_statement"]("open, high, low, close, volume")
        crossing = interpolate(candle_query, end - timedelta(minutes=3), end, "crypto_demo")
        rows = list(session.execute(crossing))
        assert len(rows) == 2
        assert {row.window_start.date().isoformat() for row in rows} == {"2026-10-23", "2026-10-24"}
        assert all(row.low <= min(row.open, row.close) <= max(row.open, row.close) <= row.high for row in rows)
        same_day = interpolate(candle_query, end.replace(minute=0), end, "crypto_demo")
        assert len(list(session.execute(same_day))) == 1
        for panel in generator["market_dashboard"]()["panels"]:
            for target in panel.get("targets", []):
                query = interpolate(target["target"], end - timedelta(minutes=3), end, "crypto_demo")
                assert list(session.execute(query)), panel["title"]
    finally:
        session.shutdown()
        cluster.shutdown()
