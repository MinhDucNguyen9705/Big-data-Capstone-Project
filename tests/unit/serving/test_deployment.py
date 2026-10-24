"""Owner E: integration contracts, UTC boundaries, safe demo data and operational validation."""

from __future__ import annotations

import importlib.util
import io
import json
import shutil
import subprocess
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from cryptopipe import contracts
from cryptopipe.serving.check import interpolate, row_count, verify
from cryptopipe.serving.seed_demo import render as seed_cql

ROOT = Path(__file__).resolve().parents[3]


def load_generator():
    spec = importlib.util.spec_from_file_location("dashboards", ROOT / "deploy/monitoring/generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_dashboards_and_sidecar_payload_are_current():
    for path, expected in load_generator().render().items():
        assert path.read_text() == expected, f"Regenerate {path}"
    configmap = yaml.safe_load((ROOT / "deploy/monitoring/dashboards.generated.yaml").read_text())
    values = yaml.safe_load((ROOT / "deploy/monitoring/values.yaml").read_text())
    sidecar = values["grafana"]["sidecar"]["dashboards"]
    assert configmap["metadata"]["labels"][sidecar["label"]] == sidecar["labelValue"]
    for filename, body in configmap["data"].items():
        assert json.loads(body) == json.loads((ROOT / "deploy/monitoring/dashboards" / filename).read_text())


def test_market_queries_obey_cassandra_contract_and_dataset_separation():
    dashboard = load_generator().market_dashboard()
    queries = [t["target"] for p in dashboard["panels"] for t in p.get("targets", [])]
    assert dashboard["timezone"] == "utc"
    for query in queries:
        assert "FROM ${dataset}." in query
        assert "symbol = '${symbol}'" in query
        assert "exchange = '${exchange}'" in query
        if contracts.CASSANDRA_OHLCV_1M in query:
            assert "day IN (" in query
            assert "window_start >= $__timeFrom" in query
            assert "window_start <= $__timeTo" in query
            assert "LIMIT 2880" in query
    variables = {v["name"]: v for v in dashboard["templating"]["list"]}
    symbols = yaml.safe_load((ROOT / "conf/symbols.yaml").read_text())["symbols"]
    assert variables["symbol"]["query"].split(",") == [s["canonical"] for s in symbols]
    assert variables["dataset"]["current"]["value"] == "crypto"


def test_cross_midnight_range_uses_both_utc_partitions_even_with_local_offsets():
    # 07:05 in Vietnam is 00:05 UTC; the prior 30 minutes straddle UTC midnight.
    end = datetime(2026, 10, 24, 7, 5, tzinfo=timezone(timedelta(hours=7)))
    start = end - timedelta(minutes=30)
    query = load_generator().candle_statement("open, high, low, close")
    actual = interpolate(query, start, end, "crypto")
    assert f"day IN (toDate((timestamp) {int(start.timestamp() * 1000)}), " in actual
    assert f"toDate((timestamp) {int(end.timestamp() * 1000)}))" in actual
    # Let Cassandra derive UTC dates from epoch milliseconds. Grafana custom
    # date formatters can use the browser's timezone independently of the dashboard.
    assert ":date:" not in query
    assert start.astimezone(UTC).date().isoformat() == "2026-10-23"
    assert end.astimezone(UTC).date().isoformat() == "2026-10-24"
    assert "FROM crypto.ohlcv_1m" in actual
    assert "${" not in actual


@pytest.mark.parametrize("hours", [0, -1, 25])
def test_market_probe_rejects_ranges_that_cannot_be_covered_by_two_days(hours):
    end = datetime(2026, 10, 24, tzinfo=UTC)
    with pytest.raises(ValueError, match="at most 24 hours"):
        interpolate("SELECT", end - timedelta(hours=hours), end, "crypto")


def test_probe_does_not_count_empty_frame_columns_as_rows():
    assert row_count({"frames": [{"data": {"values": [[], [], []]}}]}) == 0
    assert row_count({"frames": [{"data": {"values": [[1, 2], [5, 6]]}}]}) == 2


def test_live_gate_refuses_synthetic_data_before_making_requests():
    with pytest.raises(ValueError, match="synthetic"):
        verify("http://unused.invalid", "admin", "unused", "crypto_demo", require_live=True)


@pytest.mark.parametrize("empty", [False, True])
def test_http_probe_distinguishes_successful_empty_queries_from_live_readiness(monkeypatch, empty):
    generator = load_generator()

    def fake_urlopen(request, timeout):
        path = request.full_url.split("http://grafana.test", 1)[1]
        if path == "/api/health":
            body = {"database": "ok"}
        elif path.endswith("/health"):
            body = {"status": "OK"}
        elif path.startswith("/api/dashboards/"):
            dashboard = (
                generator.market_dashboard() if path.endswith("crypto-market") else generator.pipeline_dashboard()
            )
            body = {"dashboard": dashboard}
        else:
            query = json.loads(request.data)["queries"][0]
            # A stale latest trade must fail the live gate even if every panel has rows.
            name = "event_ts" if "latest_price" in query.get("target", "") else "value"
            body = {
                "results": {
                    "A": {
                        "frames": [
                            {
                                "schema": {"fields": [{"name": name}]},
                                "data": {"values": [[] if empty else [1]]},
                            }
                        ]
                    }
                },
            }
        return io.BytesIO(json.dumps(body).encode())

    monkeypatch.setattr("cryptopipe.serving.check.urlopen", fake_urlopen)
    report = verify("http://grafana.test", "admin", "unused", "crypto", require_live=False)
    assert any(line.startswith("EMPTY") for line in report) == empty
    with pytest.raises(ValueError, match="empty required panels" if empty else "stale"):
        verify("http://grafana.test", "admin", "unused", "crypto", require_live=True)


def test_seed_is_isolated_and_places_candles_in_their_utc_date_partition():
    schema = (ROOT / "cql/schema.cql").read_text()
    output = seed_cql(schema, datetime(2026, 10, 24, 0, 1, tzinfo=UTC), minutes=2)
    assert "INSERT INTO crypto." not in output
    assert "CREATE KEYSPACE IF NOT EXISTS crypto_demo" in output
    assert "'2026-10-23', '2026-10-23T23:59:00+00:00'" in output
    assert "'2026-10-24', '2026-10-24T00:00:00+00:00'" in output
    assert output.count("INSERT INTO crypto_demo.ohlcv_1m") == 2
    with pytest.raises(ValueError):
        seed_cql(schema, datetime.now(UTC), minutes=0)


def test_cassandra_service_dns_and_persistent_storage_match_existing_clients():
    services = list(yaml.safe_load_all((ROOT / "deploy/cassandra/services.yaml").read_text()))
    stateful = yaml.safe_load((ROOT / "deploy/cassandra/statefulset.yaml").read_text())
    client_env = yaml.safe_load((ROOT / "deploy/common/cryptopipe-env.yaml").read_text())["data"]
    service = services[0]
    assert f"{service['metadata']['name']}.{service['metadata']['namespace']}" == client_env["CRYPTO_CASSANDRA_HOSTS"]
    assert service["spec"]["selector"] == stateful["spec"]["selector"]["matchLabels"]
    assert stateful["spec"]["serviceName"] == services[1]["metadata"]["name"]
    volume = stateful["spec"]["volumeClaimTemplates"][0]
    mounts = stateful["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    assert {"name": volume["metadata"]["name"], "mountPath": "/var/lib/cassandra"} in mounts


def test_prometheus_discovers_crypto_monitors_without_a_helm_release_label():
    values = yaml.safe_load((ROOT / "deploy/monitoring/values.yaml").read_text())
    spec = values["prometheus"]["prometheusSpec"]
    monitor = yaml.safe_load((ROOT / "deploy/monitoring/kafka-podmonitor.yaml").read_text())
    for key, value in spec["podMonitorSelector"]["matchLabels"].items():
        assert monitor["metadata"]["labels"][key] == value
    assert not spec["podMonitorSelectorNilUsesHelmValues"]
    assert values["grafana"]["admin"]["existingSecret"] == "cryptopipe-grafana-admin"
    assert "adminPassword" not in values["grafana"]


def test_promql_parses_and_matches_actual_producer_and_spark_names(tmp_path):
    promtool = shutil.which("promtool")
    if promtool is None:
        pytest.skip("promtool not installed; serving CI supplies Prometheus 3.2.1")
    panels = load_generator().pipeline_dashboard()["panels"]
    expressions = {
        p["title"]: p["targets"][0]["expr"].replace("$__rate_interval", "2m") for p in panels if p["targets"]
    }
    rules = tmp_path / "rules.yaml"
    rules.write_text(
        yaml.safe_dump(
            {
                "groups": [
                    {
                        "name": "dashboard-validation",
                        "rules": [
                            {"record": f"dashboard_check_{i}", "expr": expr}
                            for i, expr in enumerate(expressions.values())
                        ],
                    }
                ]
            }
        )
    )
    subprocess.run([promtool, "check", "rules", str(rules)], check=True, capture_output=True, text=True)
    spark_name = "metrics_app_driver_spark_streaming_stream_ohlcv_candles_inputRate_total_Value"
    case = {
        "rule_files": [],
        "evaluation_interval": "30s",
        "tests": [
            {
                "interval": "30s",
                "input_series": [
                    {"series": 'crypto_ingest_messages_sent_total{topic="raw.trades"}', "values": "0+30x10"},
                    {
                        "series": (
                            "metrics_app_driver_spark_streaming_stream_ohlcv_candles_inputRate_total_Value"
                            '{job="cryptopipe-spark",pod="d",type="gauges"}'
                        ),
                        "values": "25+0x10",
                    },
                ],
                "promql_expr_test": [
                    {
                        "expr": f"round({expressions['Producer enqueue rate']}, 0.000001)",
                        "eval_time": "3m",
                        "exp_samples": [{"labels": '{topic="raw.trades"}', "value": 1}],
                    },
                    {
                        "expr": expressions["Spark query input rate"],
                        "eval_time": "3m",
                        "exp_samples": [
                            {
                                "labels": (
                                    f'{{__name__="{spark_name}",job="cryptopipe-spark",pod="d",'
                                    'type="gauges",query="stream_ohlcv_candles"}'
                                ),
                                "value": 25,
                            }
                        ],
                    },
                    {"expr": expressions["DLQ records / second"], "eval_time": "3m", "exp_samples": []},
                ],
            }
        ],
    }
    casefile = tmp_path / "promql.test.yaml"
    casefile.write_text(yaml.safe_dump(case))
    result = subprocess.run([promtool, "test", "rules", str(casefile)], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
