# Integration

Scope: making the layers talk to each other on the cluster (DNS, jars, configs). Main contributors: all. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-20: local Spark on Windows needs winutils, and Python workers are slow to spawn

* **Context / symptom:** `df.write.parquet(...)` failed with `HADOOP_HOME and hadoop.home.dir are unset`. After
  fixing that, each tiny unit test that used `spark.createDataFrame([...])` took ~19 s.
* **What we tried:** `scripts/setup_winutils.py` downloads `winutils.exe` + `hadoop.dll` into `.tools/hadoop`,
  which `get_spark(local=True)` picks up. Timing showed `createDataFrame` from a Python list starts one Python
  worker per partition; on this laptop (12 cores, venv inside OneDrive) each spawn costs ~0.8 s and workers are
  not reused.
* **Numbers:** one small query: 10 s with `local[*]` (12 partitions), 3.4 s with `spark.default.parallelism=4`,
  0.9 s with 1. Full unit suite: 203 s -> 37 s.
* **Fix / decision:** the test `spark` fixture sets `spark.default.parallelism=1`; `get_spark` accepts `conf`
  overrides. Linux, CI and the Docker images need none of this.
* **Lesson:** measure where local test time goes before blaming Spark; most of it was process startup.

## 2026-10-20: Hadoop does not decode %20 in file: URIs

* **Context / symptom:** `spark.read.text(Path(...).as_uri())` failed with `PATH_NOT_FOUND .../OneDrive%20-%20...`.
* **Fix / decision:** `common.spark.to_spark_path()` builds `file:///D:/One Drive/...` with literal spaces; URIs
  (`hdfs://`) pass through unchanged. Use it for every local path given to Spark.
* **Lesson:** Hadoop paths are not standard URIs; never percent-encode them.
