# Spark processing

Scope: transforms, batch jobs, joins, UDFs, query plans. Main contributors: C, D. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-22: spark-defaults.conf in the image is ignored by SparkApplications (cluster mode)

* **Context / symptom:** the sample SparkApplication `platform-hdfs-check` completed (10 000 rows written and read
  on HDFS, Kafka batch read OK), but the History Server listed nothing and `hdfs dfs -ls /spark-events` was empty,
  although the image's `/opt/spark/conf/spark-defaults.conf` sets `spark.eventLog.enabled true`.
* **What we tried:** in cluster mode the driver is started with the properties that `spark-submit` sent, from a
  generated ConfigMap mounted over `/opt/spark/conf`; the image file is never read. `spark-submit` itself runs in
  the spark-operator controller (Spark 4.0.4 image, no conf directory). Mounted `conf/spark-defaults.conf` as
  ConfigMap `spark-defaults` into the controller with `SPARK_CONF_DIR`. First attempt failed with
  `failed to write driver pod template to file: mkdir /tmp/spark: read-only file system`: setting
  `controller.volumes` in the Helm values replaces the chart's default list, which held the writable `/tmp`.
* **Numbers:** sample job 53-78 s from submit to COMPLETED; History Server lists it within 10 s
  (`spark.history.fs.update.interval=10s`).
* **Fix / decision:** `deploy/spark/operator-values.yaml` + `install-operator.sh`: ConfigMap `spark-defaults` from
  `conf/spark-defaults.conf`, `SPARK_CONF_DIR` on the controller, `/tmp` emptyDir repeated. Every job now gets UTC,
  Kryo, AQE, zstd, RocksDB state store, Prometheus servlet and the event log without listing them.
* **Lesson:** with spark-on-k8s, cluster-wide defaults belong where `spark-submit` runs (the operator), not in the
  driver image. Check effective settings in the Spark UI Environment tab (or the event log), not in the image.
