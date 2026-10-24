# Prometheus and Grafana

Owner **Chi / E**, A9. Helm chart `kube-prometheus-stack` **69.8.2**, bundled Grafana **11.5.2**,
Prometheus **3.2.1**, signed Cassandra datasource plugin **3.3.0**. Versions are pinned for reproducibility,
not claimed to be the newest release. Cassandra connects directly through the community plugin; no HTTP API
or second copy of market prices is introduced.

## Install

Requirements: the team's kind cluster, kubectl, Helm 3, Python 3.11, `make install`; default StorageClass,
internet for images/chart/plugin. Run from the repo root:

```bash
bash deploy/monitoring/install.sh
bash deploy/monitoring/port-forward.sh
```

Platform `make cluster-up` and `make deploy` already call this install hook when it exists. Grafana can start
before Cassandra; market queries become available after Cassandra and its schema Job finish. The installer
generates one random admin Secret on first install and reuses it. It does not publish credentials to stdout.

Grafana: <http://127.0.0.1:3000>; Prometheus: <http://127.0.0.1:9090> while the forwards run.
Username `admin`; read the password without putting it in Git:

```bash
kubectl --context kind-crypto -n monitoring get secret cryptopipe-grafana-admin \
  -o jsonpath='{.data.admin-password}' | base64 --decode
```

On macOS use `base64 -D` if the system utility does not accept `--decode`.
Scripts use `KUBE_CONTEXT` / `CLUSTER_NAME` like the platform scripts. `PROFILE=cloud` requires a separate
storage, retention and access review; the bundled values target the local checkpoint.

## Dashboards as code

- `/d/crypto-market`: candles, latest price, trades/minute, volume and VWAP. Select symbol, exchange and dataset.
- `/d/crypto-pipeline`: producer rates/errors/lag, Kafka committed-group lag, Spark rates/duration/state, scrape health.

Edit `generate.py`, then regenerate the committed JSON and the labelled sidecar ConfigMap:

```bash
.venv/bin/python deploy/monitoring/generate.py
.venv/bin/python deploy/monitoring/generate.py --check
```

Re-run the installer to apply them. UI edits are disabled so a dashboard is reproducible from Git.
Prometheus has 2-day / 1500 MB retention with a 2 Gi PVC; Grafana has a 1 Gi PVC. Cassandra hot candles retain
the existing 30-day TTL; these are separate retention policies.

### Query correctness

The market dashboard supports **at most 24 hours**, reading the UTC days containing range start/end. This
covers a midnight crossing without scanning the entire table. Cassandra derives those dates with
`toDate((timestamp) $__timeFrom)` / `toDate((timestamp) $__timeTo)` from the plugin's epoch-ms macros, avoiding
browser-local date formatting. Symbol fixes the partition key; the existing
clustering order is `(window_start, exchange)`, so selecting an exchange after a time range uses bounded
`ALLOW FILTERING` within those two date partitions, capped at 2880 candles. Longer historical ranges belong
in batch analytics and are explicitly unsupported in this dashboard. A manually entered range over 24 hours
will not include intermediate days. The verification tool refuses such ranges.

The current candle is provisional. Latest price is queried independently of the time picker; its timestamp
must be checked for freshness. Coinbase prices are USD mapped to canonical USDT symbols by the existing
contract; the dashboard does not perform an FX conversion. BNB/Coinbase has no configured source.

### Discovery handoff to A/B/C

Producer pods in namespace `ingest` must expose the existing Python metrics server and carry:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8000"
    prometheus.io/path: /metrics
```

Spark driver discovery uses the injected UI container port **4040**, namespace `spark`, label
`spark-role=driver`, and `/metrics/prometheus`. The platform must preserve the existing PrometheusServlet
settings in `conf/spark-defaults.conf` plus `spark.sql.streaming.metricsEnabled=true`. The separate
`/metrics/executors/prometheus` endpoint does not expose the streaming-query gauges used here.

Kafka Exporter discovery uses the existing Strimzi pod label `strimzi.io/name=crypto-kafka-exporter` and
container port `tcp-prometheus`. Its PodMonitor has the same project label as Prometheus's selector; it does
not depend on Helm's automatically generated `release` label.

The producer sent counter measures enqueue attempts, not broker acknowledgements; inspect delivery errors.
Kafka Exporter consumer lag requires committed consumer-group offsets. Spark Structured Streaming keeps
offsets in its checkpoint and does **not** commit them to Kafka. An empty Kafka lag panel is not proof of zero
Spark backlog. Native Spark `latency` is trigger duration in ms (dashboard converts it to seconds), not
end-to-end trade-to-dashboard latency. Empty metrics remain **No data**, never a fabricated zero.

## Validation

```bash
# Offline generation/shell checks plus Helm/Kubernetes schemas (downloads chart/schemas):
bash deploy/monitoring/validate.sh  # helm, kubectl, kubeconform 0.6.7 required
.venv/bin/python -m pytest tests/unit/serving/test_deployment.py -q

# Exact market CQL against an isolated Cassandra (writes crypto_demo, not crypto):
CRYPTO_SERVING_TEST_HOST=127.0.0.1 .venv/bin/python -m pytest \
  -m integration tests/integration/test_serving.py -q

# HTTP checks against the actual provisioned Grafana, with both port forwards running:
export GRAFANA_PASSWORD="$(kubectl --context kind-crypto -n monitoring get secret \
  cryptopipe-grafana-admin -o jsonpath='{.data.admin-password}' | base64 --decode)"
.venv/bin/python -m cryptopipe.serving.check
.venv/bin/python -m cryptopipe.serving.check --dataset crypto_demo
.venv/bin/python -m cryptopipe.serving.check --require-live
unset GRAFANA_PASSWORD
```

The HTTP tool checks datasource health, provisioned dashboards, and executes every panel query. Non-strict
checks report EMPTY separately. The live gate requires recent market rows, a latest-trade timestamp within
5 minutes, and currently queryable producer/Spark metrics. It refuses the synthetic dataset. It does not
replace a Kafka-to-Cassandra end-to-end integration test or a sustained workload measurement.

`serving.yml` in GitHub Actions validates all rendered Kubernetes resources, including custom-resource
schemas from the pinned chart, and uses promtool 3.2.1 to parse all dashboard expressions and test known samples.

## Troubleshooting

| Symptom | Check |
|---|---|
| Cassandra datasource fails | Cassandra/schema Job ready; `cassandra.storage:9042`; plugin installed and signed |
| Plugin download fails | Grafana pod logs and outbound access to grafana.com; rerun deployment after network recovery |
| Market empty | Live vs synthetic dataset, configured symbol/exchange, time range and UTC day; inspect `latest_price` |
| Pipeline empty | Prometheus Targets, producer annotations, Spark 4040/servlet, Strimzi exporter and selectors |
| Grafana dashboards missing | `cryptopipe-dashboards` ConfigMap label, sidecar logs, `generate.py --check` |
| UI not reachable | Run `port-forward.sh`; NodePort 30300 maps to laptop 3000 in the team's kind config |
| Grafana login fails after an upgrade | Existing Secret is reused; changing a Secret alone does not rotate an existing Grafana DB password |

Primary references: [Cassandra plugin provisioning](https://github.com/HadesArchitect/GrafanaCassandraDatasource/blob/v3.3.0/docs/provisioning.md),
[plugin query model](https://github.com/HadesArchitect/GrafanaCassandraDatasource/blob/v3.3.0/src/models.ts),
[Spark MetricsReporter](https://github.com/apache/spark/blob/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/MetricsReporter.scala),
[Cassandra date/time functions](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/functions.html),
[Helm chart](https://github.com/prometheus-community/helm-charts/tree/kube-prometheus-stack-69.8.2/charts/kube-prometheus-stack).
