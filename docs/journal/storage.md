# Storage

Scope: HDFS, Parquet layout, partitioning, small files, Cassandra modelling. Main contributors: A, D. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-20: the `date` partition column is read back as DateType

* **Context / symptom:** the demo writes `date` as a string `yyyy-MM-dd` partition; reading the directory returns
  `datetime.date` values because Spark's partition discovery infers types.
* **Fix / decision:** documented in [contracts.md](../contracts.md#4-lake-layout); filters like
  `F.col("date") == "2025-10-09"` still work through implicit casting. Jobs that need the string type pass a schema
  or set `spark.sql.sources.partitionColumnTypeInference.enabled=false`.
* **Lesson:** a partition column's type is decided by the reader, not the writer.
