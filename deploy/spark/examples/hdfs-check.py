"""Platform sample job (owner A): proves a SparkApplication can write and read the lake on HDFS and that the
Kafka connector is in the image. Not part of the pipeline; lives in deploy/, not src/.

Writes a small DataFrame as Parquet to ${CRYPTO_LAKE_ROOT}/_platform_check/<timestamp>, reads it back, then
reads raw.trades with the batch Kafka source (0 rows is fine: it proves the jar and the bootstrap address).
"""

from __future__ import annotations

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cryptopipe.config import settings
from cryptopipe.contracts import TOPIC_RAW_TRADES


def main() -> int:
    spark = SparkSession.builder.appName("platform-hdfs-check").getOrCreate()
    out = settings.lake_path("_platform_check", time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))

    df = spark.range(0, 10_000, numPartitions=4).withColumn("symbol", F.concat(F.lit("SYM-"), F.col("id") % 10))
    df.write.mode("overwrite").parquet(out)
    rows = spark.read.parquet(out).count()
    print(f"HDFS: wrote and read {rows} rows at {out}")
    if rows != 10_000:
        print("PLATFORM CHECK FAILED: row count mismatch")
        return 1

    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", TOPIC_RAW_TRADES.name)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    print(f"Kafka: {TOPIC_RAW_TRADES.name} has {kafka.count()} messages ({settings.kafka_bootstrap})")
    print("PLATFORM CHECK OK")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
