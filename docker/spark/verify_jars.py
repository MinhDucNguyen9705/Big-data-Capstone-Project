"""Check that the Spark image has everything a job needs without `--packages`. Owner: A (Platform).

    docker run --rm cryptopipe-spark:latest /opt/spark/bin/spark-submit /opt/cryptopipe/verify_jars.py

Starts a local SparkSession (event log off: HDFS is not reachable outside the cluster), loads the connector
classes through the JVM, and imports cryptopipe. Exits 1 on the first missing piece.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

CLASSES = {
    "Kafka source/sink": "org.apache.spark.sql.kafka010.KafkaSourceProvider",
    "Kafka token provider": "org.apache.spark.kafka010.KafkaTokenUtil",
    "kafka-clients": "org.apache.kafka.clients.producer.KafkaProducer",
    "commons-pool2": "org.apache.commons.pool2.impl.GenericKeyedObjectPool",
    "Cassandra connector": "com.datastax.spark.connector.cql.CassandraConnector",
    "Cassandra catalog": "com.datastax.spark.connector.datasource.CassandraCatalog",
    "GraphFrames": "org.graphframes.GraphFrame",
    "HDFS client": "org.apache.hadoop.hdfs.DistributedFileSystem",
}


def main() -> int:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("verify-jars")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    failed = 0
    for what, cls in CLASSES.items():
        try:
            spark._jvm.java.lang.Class.forName(cls)
            print(f"ok    {what:22s} {cls}")
        except Exception as exc:  # noqa: BLE001 - report every missing class
            print(f"FAIL  {what:22s} {cls}: {str(exc).splitlines()[0]}")
            failed += 1

    import cryptopipe
    from cryptopipe.jobs import JOBS

    print(f"ok    cryptopipe {getattr(cryptopipe, '__version__', '')} jobs: {', '.join(JOBS)}")
    print(f"ok    python {sys.version.split()[0]}, spark {spark.version}")
    if sys.version_info[:2] != (3, 11):
        print("FAIL  python must be 3.11 (pyproject requires-python)")
        failed += 1
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
