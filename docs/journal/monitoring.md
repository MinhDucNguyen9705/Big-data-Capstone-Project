# Monitoring

Scope: Prometheus, Grafana, metrics, alerting, logs. Main contributors: E. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-24: Helm commonLabels collided with a chart's own part-of label

* **Context / symptom:** adding `app.kubernetes.io/part-of=crypto-pipeline` through `commonLabels` made strict
  validation fail on duplicate YAML keys already emitted by kube-prometheus-stack.
* **What we tried:** Helm lint alone passed. Validated the complete Helm output with kubeconform and the
  pinned chart's CRD schemas; this exposed the duplicate labels.
* **Numbers:** the failing render had 80 resources, 50 parser errors. After changing the common label to
  `cryptopipe.io/monitoring=true`, all 80 resources validated, 0 errors, 0 skipped.
* **Fix / decision:** use a project-specific common label for the chart's ServiceMonitor selector; keep the
  project's existing part-of label for the separately supplied Kafka PodMonitor.
* **Lesson:** successful Helm rendering does not establish that the emitted YAML is valid Kubernetes input.

## 2026-10-24: dashboard labels must describe what the metrics really measure

* **Context / symptom:** source inspection showed that `crypto_ingest_messages_sent` increments after enqueue,
  while Kafka acknowledgements are handled separately. Spark's native metric named `latency` is trigger
  duration; its Kafka source does not commit consumer offsets for Kafka Exporter's lag metric.
* **What we tried:** compared the producer source and Apache Spark MetricsReporter/StreamExecution with the
  planned dashboard panels. Used promtool with known producer counters and Spark gauge names.
* **Numbers:** all 12 pipeline expressions parse; sample tests cover a one-message/second producer, a
  25-row/second Spark query and an absent DLQ series. These are synthetic test samples, not workload measurements.
* **Fix / decision:** label enqueue rate and trigger duration explicitly; document committed-group lag;
  preserve missing series as No data. Link primary sources in the monitoring README.
* **Lesson:** a plausible-looking dashboard can still be wrong if its metric semantics are inferred from names.
