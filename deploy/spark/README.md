# deploy/spark/

Spark on Kubernetes with the kubeflow spark-operator. Owner: **A (Platform)** for everything here except
`stream-*.yaml` (**C**) and `batch-*.yaml` (**D**), which `make deploy WITH_JOBS=1` applies.

| File | What |
|---|---|
| `install-operator.sh`, `operator-values.yaml` | spark-operator chart 2.5.2 in namespace `spark`, jobs only in `spark`, webhook on, metrics on. The chart creates ServiceAccount **`spark`** with its Role (pods, services, configmaps, PVCs), so there is no separate `rbac.yaml` |
| `history-server.yaml` | Spark History Server on `hdfs://hdfs-namenode.storage:8020/spark-events`, NodePort 31808 → <http://localhost:18080> |
| `examples/` | sample `SparkApplication` + job to copy for your own jobs ([examples/README.md](examples/README.md)) |
| `check.sh` | submits the sample, waits for `COMPLETED`, prints the driver log tail, checks the History Server API |

ConfigMap **`hadoop-conf`** (namespace `spark`) holds `core-site.xml` and `hdfs-site.xml`. It is copied from
`hdfs-config` in `storage` by `deploy/scripts/deploy-all.sh` (function `sync_hadoop_conf`), never written by hand.
SparkApplications mount it with `spec.hadoopConfigMap: hadoop-conf`.

Image `cryptopipe-spark` ([docker/spark/Dockerfile](../../docker/spark/Dockerfile)): Spark 3.5.9, Scala 2.12, Java 17,
Python 3.11, the jars in `docker/spark/jars.txt`, the cryptopipe wheel, `conf/spark-defaults.conf`, and one
entrypoint per job at `local:///opt/cryptopipe/jobs/<name>.py`.
