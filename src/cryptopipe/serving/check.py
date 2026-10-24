# Owner: E, A9. Checks actual Grafana datasources and every dashboard query over HTTP.
"""Verify provisioned dashboards. Credentials: GRAFANA_USER / GRAFANA_PASSWORD (never printed)."""

from __future__ import annotations

import argparse
import base64
import json
import os
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def interpolate(statement: str, start: datetime, end: datetime, dataset: str) -> str:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    if not timedelta(0) < end - start <= timedelta(days=1):
        raise ValueError("market range must be positive and at most 24 hours")
    if dataset not in {"crypto", "crypto_demo"}:
        raise ValueError("dataset must be crypto or crypto_demo")
    replacements = {
        "${symbol}": "BTC-USDT",
        "${exchange}": "binance",
        "${dataset}": dataset,
        "$__timeFrom": str(int(start.timestamp() * 1000)),
        "$__timeTo": str(int(end.timestamp() * 1000)),
    }
    for variable, value in replacements.items():
        statement = statement.replace(variable, value)
    return statement


def row_count(result: dict) -> int:
    """Grafana returns values column-wise; do not confuse field count with row count."""
    count = 0
    for frame in result.get("frames", []):
        values = frame.get("data", {}).get("values", [])
        if values:
            count += len(values[0])
    return count


def verify(url: str, username: str, password: str, dataset: str, require_live: bool) -> list[str]:
    authorization = base64.b64encode(f"{username}:{password}".encode()).decode()

    def request(path: str, payload: dict | None = None) -> dict:
        body = json.dumps(payload).encode() if payload is not None else None
        req = Request(
            url.rstrip("/") + path,
            data=body,
            headers={"Authorization": f"Basic {authorization}", "Content-Type": "application/json"},
        )
        with urlopen(req, timeout=45) as response:
            return json.load(response)

    if require_live and dataset != "crypto":
        raise ValueError("--require-live cannot be satisfied by synthetic crypto_demo data")
    report = []
    health = request("/api/health")
    if health.get("database") != "ok":
        raise ValueError("Grafana database is not healthy")
    for uid in ("cassandra", "prometheus"):
        health = request(f"/api/datasources/uid/{uid}/health")
        if health.get("status") != "OK":
            raise ValueError(f"Datasource {uid} is not healthy")
        report.append(f"PASS datasource {uid}")
    end = datetime.now(UTC)
    start = end - timedelta(minutes=30)
    nonempty: dict[str, bool] = {}
    latest_event_ms = 0
    for uid in ("crypto-market", "crypto-pipeline"):
        saved = request(f"/api/dashboards/uid/{uid}")["dashboard"]
        for panel in saved["panels"]:
            for target in panel.get("targets", []):
                target = dict(target)
                if "target" in target:
                    target["target"] = interpolate(target["target"], start, end, dataset)
                if "expr" in target:
                    target["expr"] = target["expr"].replace("$__rate_interval", "2m")
                    if require_live:
                        target.update({"range": False, "instant": True})
                target["intervalMs"] = 30000
                target["maxDataPoints"] = 100
                response = request(
                    "/api/ds/query",
                    {
                        "from": str(int(start.timestamp() * 1000)),
                        "to": str(int(end.timestamp() * 1000)),
                        "queries": [target],
                    },
                )
                results = response.get("results", {})
                if "A" not in results:
                    raise ValueError(f"Missing query response: {uid}/{panel['title']}")
                result = results["A"]
                if result.get("error") or result.get("status", 200) >= 400:
                    raise ValueError(f"Query failed: {uid}/{panel['title']}: {result.get('error', 'unknown error')}")
                rows = row_count(result)
                nonempty[panel["title"]] = rows > 0
                if panel["title"] == "Latest trade · check event_ts":
                    for frame in result.get("frames", []):
                        fields = frame.get("schema", {}).get("fields", [])
                        for index, field in enumerate(fields):
                            if field["name"] == "event_ts":
                                timestamps = frame["data"]["values"][index]
                                latest_event_ms = max([latest_event_ms, *[t for t in timestamps if t is not None]])
                report.append(f"{'PASS' if rows else 'EMPTY'} {uid}/{panel['title']}: {rows} rows")
    if require_live:
        required = [
            "${symbol} · ${exchange} · 1-minute candles",
            "Latest trade · check event_ts",
            "Producer enqueue rate",
            "Spark query input rate",
            "Spark trigger execution duration",
        ]
        missing = [title for title in required if not nonempty.get(title)]
        if missing:
            raise ValueError("Live gate has empty required panels: " + ", ".join(missing))
        lag = end.timestamp() - latest_event_ms / 1000
        if not -300 <= lag <= 300:
            raise ValueError("Latest trade is stale or has an invalid future timestamp (more than 5 minutes)")
    report.append(
        "PASS live data gate"
        if require_live
        else "Query checks complete; empty metrics are not proof of live readiness"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:3000")
    parser.add_argument("--dataset", choices=["crypto", "crypto_demo"], default="crypto")
    parser.add_argument("--require-live", action="store_true")
    args = parser.parse_args()
    password = os.environ.get("GRAFANA_PASSWORD")
    if not password:
        parser.exit(2, "Set GRAFANA_PASSWORD from the Kubernetes Secret; see deploy/monitoring/README.md\n")
    try:
        for line in verify(
            args.url, os.environ.get("GRAFANA_USER", "admin"), password, args.dataset, args.require_live
        ):
            print(line)
    except (HTTPError, URLError, ValueError, KeyError) as exc:
        parser.exit(1, f"Serving check failed: {exc}\n")


if __name__ == "__main__":
    main()
