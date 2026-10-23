# deploy/spark/examples/

Owner: **A (Platform)**. A working SparkApplication you copy for your own jobs.
`deploy/spark/check.sh` submits it on every `make smoke`, so it stays known-good.

| File | What |
|---|---|
| `hdfs-check.py` | Sample job: writes and reads Parquet under `${CRYPTO_LAKE_ROOT}/_platform_check/`, reads `raw.trades` with the batch Kafka source |
| `hdfs-check.yaml` | Its `SparkApplication` (operator `sparkoperator.k8s.io/v1beta2`) |

## Copying it for a pipeline job

C copies it to `deploy/spark/stream-<job>.yaml`, D to `deploy/spark/batch-<job>.yaml`. `make deploy WITH_JOBS=1`
applies those files. Change only the fields marked `CHANGE`:

| Field | Sample | Your job |
|---|---|---|
| `metadata.name` | `platform-hdfs-check` | e.g. `stream-bronze`, `batch-silver` |
| `mainApplicationFile` | `local:///opt/platform-check/hdfs-check.py` | `local:///opt/cryptopipe/jobs/<name>.py`: one file per entry of `cryptopipe.jobs.JOBS`, generated in the image (calls `cryptopipe.jobs.<name>.main(argv)`) |
| `arguments` | `[]` | your CLI arguments, e.g. `["--watermark", "10 minutes"]` |
| `driver` / `executor` `cores`, `memory`, `instances` | 1 core / 1 g each | size for the job; on a laptop keep driver + executors under ~4 GB in total |
| `restartPolicy` | `Never` | streaming: `type: Always` (or `OnFailure` with `onFailureRetries`); batch: `Never` or `OnFailure` |
| `volumes`, `driver.volumeMounts` | mounts the sample script | delete them (your code is in the image) |
| `sparkConf` | | job settings that must not be global, e.g. `spark.sql.shuffle.partitions` |

Keep as they are: `image: cryptopipe-spark:latest`, `imagePullPolicy: IfNotPresent`, `hadoopConfigMap: hadoop-conf`,
`serviceAccount: spark`, `envFrom: cryptopipe-env` on driver and executor, and the Prometheus annotations.
Cluster-wide defaults (event log to HDFS, UTC, Prometheus servlet, Kryo, zstd) come from `conf/spark-defaults.conf`
in the image; do not repeat them.

**Batch jobs on a schedule** wrap the same spec in a `ScheduledSparkApplication`:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: ScheduledSparkApplication
metadata: { name: batch-silver, namespace: spark }
spec:
  schedule: "15 0 * * *"           # UTC
  concurrencyPolicy: Forbid
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 3
  template:
    # everything under `spec:` of hdfs-check.yaml (type, mode, image, mainApplicationFile, driver, executor, ...)
```

**Run and debug:**

```bash
kubectl apply -f deploy/spark/batch-silver.yaml
kubectl -n spark get sparkapplications                  # STATUS: SUBMITTED -> RUNNING -> COMPLETED / FAILED
kubectl -n spark logs -f <name>-driver                  # driver log
kubectl -n spark describe sparkapplication <name>       # events: why it failed to start
make port-forward                                       # Spark UI of the running driver on localhost:4040
```

Finished runs are in the History Server: <http://localhost:18080>.
