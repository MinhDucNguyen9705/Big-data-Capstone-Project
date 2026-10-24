#!/usr/bin/env python3
"""Owner E: reproducible Grafana dashboards and their sidecar ConfigMap; --check never writes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / "deploy/monitoring"
CASSANDRA = {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra"}
PROMETHEUS = {"type": "prometheus", "uid": "prometheus"}


def dashboard(title: str, uid: str) -> dict:
    return {
        "id": None,
        "uid": uid,
        "title": title,
        "tags": ["cryptopipe", "checkpoint-1"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "refresh": "30s",
        "time": {"from": "now-30m", "to": "now"},
        "timepicker": {
            "refresh_intervals": ["10s", "30s", "1m", "5m"],
            "time_options": ["5m", "15m", "30m", "1h", "6h", "12h", "24h"],
        },
        "templating": {"list": []},
        "annotations": {"list": []},
        "panels": [],
    }


def panel(number: int, title: str, kind: str, x: int, y: int, w: int = 12, h: int = 8) -> dict:
    return {
        "id": number,
        "title": title,
        "type": kind,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"noValue": "No data", "mappings": []}, "overrides": []},
        "options": {},
        "targets": [],
    }


def note(number: int, text: str, y: int, h: int = 3) -> dict:
    out = panel(number, "How to read this dashboard", "text", 0, y, 24, h)
    out["options"] = {"mode": "markdown", "content": text}
    return out


def custom_variable(name: str, label: str, values: list[str]) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "custom",
        "query": ",".join(values),
        "multi": False,
        "includeAll": False,
        "current": {"text": values[0], "value": values[0]},
        "options": [{"text": v, "value": v, "selected": i == 0} for i, v in enumerate(values)],
    }


def cassandra_target(statement: str) -> dict:
    # These are the plugin 3.3.0 field names, not SQL/Prometheus target fields.
    return {
        "refId": "A",
        "datasource": CASSANDRA,
        "queryType": "query",
        "rawQuery": True,
        "target": statement,
    }


def candle_statement(columns: str) -> str:
    # At most two UTC date partitions for the supported <=24 h range. Exchange is
    # after window_start in the existing clustering key, so bounded filtering is necessary.
    return (
        f"SELECT exchange, window_start, {columns} FROM ${{dataset}}.ohlcv_1m "
        "WHERE symbol = '${symbol}' "
        "AND day IN (toDate((timestamp) $__timeFrom), toDate((timestamp) $__timeTo)) "
        "AND window_start >= $__timeFrom AND window_start <= $__timeTo "
        "AND exchange = '${exchange}' LIMIT 2880 ALLOW FILTERING"
    )


def market_dashboard() -> dict:
    out = dashboard("Crypto · Market", "crypto-market")
    symbols = yaml.safe_load((ROOT / "conf/symbols.yaml").read_text())
    # symbols.yaml is the contract, not a manually maintained dashboard symbol list.
    tracked = [row["canonical"] for row in symbols["symbols"]]
    out["templating"]["list"] = [
        custom_variable("symbol", "Symbol", tracked),
        custom_variable("exchange", "Exchange", ["binance", "coinbase"]),
        custom_variable("dataset", "Dataset (crypto = LIVE; crypto_demo = SYNTHETIC)", ["crypto", "crypto_demo"]),
    ]
    out["panels"].append(
        note(
            1,
            "**UTC · supported range: at most 24 hours · selected dataset: ${dataset}.** "
            "crypto is the live pipeline; crypto_demo contains synthetic demonstration data only. "
            "Queries read the start/end UTC day partitions, including midnight crossings. "
            "Longer ranges are not supported here; use the batch lake for history. "
            "The current minute is provisional. Latest price ignores the time picker; "
            "check its event timestamp for freshness. Coinbase USD is mapped to the canonical USDT symbol "
            "by the project contract; prices are not FX-converted. Empty results remain No data.",
            0,
            4,
        )
    )
    candles = panel(2, "${symbol} · ${exchange} · 1-minute candles", "candlestick", 0, 4, 16, 11)
    candles["datasource"] = CASSANDRA
    candles["targets"] = [cassandra_target(candle_statement("open, high, low, close, volume"))]
    candles["transformations"] = [{"id": "sortBy", "options": {"sort": [{"field": "window_start", "desc": False}]}}]
    candles["fieldConfig"]["defaults"].update({"unit": "currencyUSD", "decimals": 4})
    candles["options"] = {
        "mode": "candles",
        "candleStyle": "candles",
        "colorStrategy": "open-close",
        "fields": {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"},
        "legend": {"displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "single"},
    }
    out["panels"].append(candles)
    latest = panel(3, "Latest trade · check event_ts", "table", 16, 4, 8, 5)
    latest["datasource"] = CASSANDRA
    latest["targets"] = [
        cassandra_target(
            "SELECT exchange, symbol, price, qty, event_ts FROM ${dataset}.latest_price "
            "WHERE symbol = '${symbol}' AND exchange = '${exchange}'"
        )
    ]
    latest["options"] = {"showHeader": True, "cellHeight": "sm"}
    out["panels"].append(latest)
    for number, title, col, unit, x, y, w, h in [
        (4, "Trades per minute", "trades", "short", 16, 9, 8, 6),
        (5, "Traded volume per minute", "volume", "short", 0, 15, 12, 7),
        (6, "Volume-weighted average price", "vwap", "currencyUSD", 12, 15, 12, 7),
    ]:
        graph = panel(number, title, "timeseries", x, y, w, h)
        graph["datasource"] = CASSANDRA
        graph["targets"] = [cassandra_target(candle_statement(col))]
        graph["transformations"] = candles["transformations"]
        graph["fieldConfig"]["defaults"].update({"unit": unit, "min": 0})
        graph["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}}
        out["panels"].append(graph)
    return out


def spark_metric(suffix: str) -> str:
    regex = f"metrics_.*_spark_streaming_(.*)_{suffix}_Value"
    return (
        'label_replace({job="cryptopipe-spark",__name__=~"' + regex + '"}, "query", "$1", "__name__", "' + regex + '")'
    )


def pipeline_dashboard() -> dict:
    out = dashboard("Crypto · Pipeline", "crypto-pipeline")
    out["panels"].append(
        note(
            1,
            "**No data is not zero.** Empty panels mean missing discovery, scrape failures, "
            "or a metric not yet emitted. Producer sent counters measure enqueue attempts, "
            "not broker acknowledgements; inspect delivery errors too. "
            "Kafka Exporter lag only covers groups with committed offsets; "
            "Spark Structured Streaming stores its offsets in checkpoints and does not commit them to Kafka. "
            "Spark input/processing rates are per query, so downstream handoff rows are not new source trades. "
            "Batch duration is trigger execution time, not end-to-end latency.",
            0,
            5,
        )
    )
    specs = [
        ("Producer enqueue rate", "sum by (topic) (rate(crypto_ingest_messages_sent_total[$__rate_interval]))", "ops"),
        ("DLQ records / second", "sum by (source) (rate(crypto_ingest_dlq_total[$__rate_interval]))", "ops"),
        (
            "Kafka delivery errors / second",
            "sum by (topic) (rate(crypto_ingest_delivery_errors_total[$__rate_interval]))",
            "ops",
        ),
        ("Producer event lag", "max by (source) (crypto_ingest_event_lag_seconds)", "s"),
        ("Kafka committed consumer-group lag", "sum by (consumergroup, topic) (kafka_consumergroup_lag)", "short"),
        (
            "WebSocket reconnects / second",
            "sum by (source) (rate(crypto_ingest_reconnects_total[$__rate_interval]))",
            "ops",
        ),
        ("Spark query input rate", spark_metric("inputRate_total"), "ops"),
        ("Spark query processing rate", spark_metric("processingRate_total"), "ops"),
        ("Spark trigger execution duration", f"({spark_metric('latency')}) / 1000", "s"),
        ("Spark state rows", spark_metric("states_rowsTotal"), "short"),
        ("Spark state memory", spark_metric("states_usedBytes"), "bytes"),
        ("Scrape health (0 down / 1 up)", 'up{namespace=~"ingest|spark|kafka"}', "short"),
    ]
    for i, (title, expression, unit) in enumerate(specs):
        graph = panel(i + 2, title, "timeseries", (i % 2) * 12, 5 + (i // 2) * 8)
        graph["datasource"] = PROMETHEUS
        graph["targets"] = [
            {
                "refId": "A",
                "datasource": PROMETHEUS,
                "expr": expression,
                "range": True,
                "legendFormat": "{{query}}{{source}}{{topic}}{{consumergroup}} {{pod}}",
            }
        ]
        graph["fieldConfig"]["defaults"].update({"unit": unit, "min": 0})
        graph["options"] = {
            "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull"]},
            "tooltip": {"mode": "multi"},
        }
        out["panels"].append(graph)
    return out


def render() -> dict[Path, str]:
    dashboards = {"market.json": market_dashboard(), "pipeline.json": pipeline_dashboard()}
    encoded = {name: json.dumps(value, ensure_ascii=False, indent=2) + "\n" for name, value in dashboards.items()}
    result = {DEST / "dashboards" / name: body for name, body in encoded.items()}
    # JSON is valid YAML and avoids version-dependent YAML block-scalar output.
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cryptopipe-dashboards",
            "namespace": "monitoring",
            "labels": {"grafana_dashboard": "1", "app.kubernetes.io/part-of": "crypto-pipeline"},
        },
        "data": encoded,
    }
    result[DEST / "dashboards.generated.yaml"] = json.dumps(configmap, ensure_ascii=False, indent=2) + "\n"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if committed outputs differ; do not write")
    args = parser.parse_args()
    stale = []
    for path, content in render().items():
        if args.check:
            if not path.exists() or path.read_text() != content:
                stale.append(str(path.relative_to(ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    if stale:
        parser.exit(1, "Regenerate with python deploy/monitoring/generate.py: " + ", ".join(stale) + "\n")
    print("Grafana dashboards and ConfigMap match the generator" if args.check else "Generated Grafana dashboards")


if __name__ == "__main__":
    main()
