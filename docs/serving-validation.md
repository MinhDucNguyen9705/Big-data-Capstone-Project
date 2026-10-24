# Serving validation evidence — 24/10/2026

Branch: `e/serving-and-docs`. Base: `main` at `b7edd41`.
This record distinguishes source validation, local processes and acceptance on the team's Kubernetes cluster.

## Environment

- macOS arm64; Python 3.11.13; PySpark 3.5.9; JDK 17.
- Helm 3.17.3; kubeconform 0.6.7; promtool 3.2.1.
- Helm chart kube-prometheus-stack 69.8.2, bundled Grafana 11.5.2; Cassandra plugin 3.3.0.
- Docker CLI is present but its daemon is not running. The Docker application bundle could not be started
  (`kLSNoExecutableErr`). `kubectl config current-context` reports no configured context.
- Validation tools and JDK were downloaded into a temporary folder. The repo's `.venv` is not a deliverable;
  recreate the environment with `make install` on another machine.

## Completed checks

| Check | Actual outcome |
|---|---|
| `make lint` | Ruff check and format check pass |
| `make test`, Python 3.11 / JDK 17, promtool on PATH | **171 passed, 3 deselected**, 37.82 s |
| Serving deployment/query unit checks alone | **14 passed**, including promtool samples |
| `python deploy/monitoring/generate.py --check` | Generated dashboard JSON and ConfigMap match |
| `helm lint` against the pinned chart and project values | 1 chart linted, 0 failed |
| `deploy/monitoring/validate.sh` | **80 resources valid, 0 invalid, 0 errors, 0 skipped** |
| Core resource validation | Kubernetes 1.32.0 strict schemas |
| Custom-resource validation | OpenAPI schemas extracted from the exact chart's CRD bundle |
| Shell scripts | `bash -n` passes as part of validation |
| Walking skeleton, isolated temporary lake | **500 input, 3 invalid, 5 duplicates removed, 60 candles** |
| Real Cassandra 5.0.6 query integration | **1 passed**, 1.05 s after service startup |
| Real Spark 3.5.9 streaming metrics endpoint | All **5** dashboard metric families found at `/metrics/prometheus` |

The full unit-test command deselects integration tests that require external services. The separate
Cassandra-only integration gate below was run; the Kafka/HDFS pipeline integration gates were not run here.
The initial sandboxed Spark attempt could not bind a local socket; rerunning with local-socket access produced
the passing result above. PromQL tests use synthetic samples, not a measured market workload.

The schema check caught a duplicate Helm label and a PyYAML enum parsing difference; both were corrected and
the final 80-resource check passed. See the dated monitoring/quality-testing journal entries.

## Real local Cassandra gate

Started the official Cassandra 5.0.6 binary in a temporary directory, bound to localhost, with JDK 17 and
a 768 MiB heap. No existing database or team cluster was used. Ran:

```bash
CRYPTO_SERVING_TEST_HOST=127.0.0.1 .venv/bin/python -m pytest \
  -m integration tests/integration/test_serving.py -q
```

Outcome: **1 passed in 1.05 s**. This applies the demo schema/writes twice, checks the exact generated candle
query across UTC midnight and within one day (including duplicate date values in `IN`), and executes every
market panel's query. Verified two candles across the two date partitions and one candle for the same-day
range. All writes use `crypto_demo`. The database was a temporary verification process, not a Kubernetes pod.

A native Grafana package download was attempted for additional UI/API verification but timed out. Plugin
source/model and provisioning were inspected, but no successful native Grafana run or screenshot is claimed.

## Real local Spark metric check

A temporary rate-source streaming query, `serving_metrics_probe`, was started with the same servlet settings
used by the platform. A real HTTP request to the driver UI's `/metrics/prometheus` returned the exact five
families used by the generated dashboard: `inputRate_total_Value`, `processingRate_total_Value`,
`latency_Value`, `states_rowsTotal_Value`, `states_usedBytes_Value`, each under the normalized
`metrics_..._spark_streaming_serving_metrics_probe_...` prefix. The check passed and the query/session stopped.
This verifies metric names and servlet configuration, not Kubernetes target discovery or real market load.

Selected command output is saved in [evidence/serving-20261024.txt](evidence/serving-20261024.txt).

## Cluster acceptance still required

No Kubernetes cluster was created in this environment. The following must be recorded on the team's cluster:

- Cassandra readiness, schema Job and persistent-volume recovery after pod recreation.
- Grafana datasource health and UI rendering with the pinned chart's actual image/plugin.
- All market queries returning live Spark results, including a UTC-midnight range when suitable data exists.
- Prometheus discovery of B's annotated producers, C's driver servlet and Strimzi's Kafka Exporter.
- `cryptopipe.serving.check --require-live` and the group's end-to-end demo.
- Sustained latency/throughput measurements, screenshots and actual full-dataset profile.

The installers, independent synthetic dataset and executable HTTP checks are supplied for those gates. The
remaining acceptance checkboxes in `checkpoint-1.md` intentionally have not been ticked from static results.
No GitHub push, pull request or live deployment is claimed by this record.
