# Spark applications

Speed-layer manifests: `stream-bronze.yaml`, `stream-ohlcv.yaml`, shared `stream-config.yaml` and
`stream-rbac.yaml` (C, coordinated with A). Other platform/batch deployments remain A/D's tasks.

Prerequisites: namespace `spark`, Spark Operator with the `SparkApplication` CRD, built/loaded
`cryptopipe-spark:dev` image, Kafka/topics, HDFS, Cassandra and applied `cql/schema.cql`.

1. Edit `stream-config.yaml` for the platform's service names and endpoints.
2. Create `/spark-events` on HDFS and allow Spark's user (UID 185 in the image) to write it. Allow access to
   the configured lake/checkpoint roots too. Enable the History Server against that same event-log directory.
3. Apply RBAC and environment, then the applications:

```bash
kubectl apply -f deploy/spark/stream-rbac.yaml -f deploy/spark/stream-config.yaml
kubectl apply -f deploy/spark/stream-bronze.yaml -f deploy/spark/stream-ohlcv.yaml
kubectl get sparkapplications -n spark
kubectl logs -n spark -f stream-ohlcv-driver
```

Each application uses 1 driver and 1 executor. The two together need about 5 GiB JVM heap plus overhead;
include Kafka/HDFS/Cassandra/monitoring when budgeting kind memory. Resource tuning belongs in these YAMLs.
Connector jars come from `docker/spark/Dockerfile`, never runtime downloads in job code.
The JSON batch progress log includes query, batch id, input rows, watermark, rows dropped by watermark,
state rows and duration. Spark metrics are available through the driver's Prometheus servlet; platform A/E
must configure scraping. Local file-stream runs: [docs/getting-started.md](../../docs/getting-started.md), step A7.
