# Owner: C (Speed layer) | Task: A6
"""Source, checkpoint and lifecycle helpers shared by the two streaming applications."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from cryptopipe import contracts
from cryptopipe.common.logging import get_logger
from cryptopipe.common.spark import to_spark_path
from cryptopipe.config import Settings
from cryptopipe.schemas import Trade

LOG = get_logger(__name__)


def parser(job: str, trigger: str) -> argparse.ArgumentParser:
    out = argparse.ArgumentParser(prog=job, description=__doc__)
    out.add_argument("--local", action="store_true", help="Use local Spark (does not change the sink)")
    out.add_argument("--source", help="JSONL file/directory/glob instead of Kafka; one contract JSON per line")
    out.add_argument("--once", action="store_true", help="Drain available input with AvailableNow, then exit")
    out.add_argument("--trigger", default=trigger, help="Processing-time interval, e.g. '10 seconds'")
    out.add_argument("--starting-offsets", default="earliest", help="Kafka initial offsets; ignored on recovery")
    out.add_argument("--max-offsets-per-trigger", type=int, default=100000)
    out.add_argument("--max-files-per-trigger", type=int, default=1)
    return out


def read_source(spark: SparkSession, settings: Settings, topic: str, args: argparse.Namespace) -> DataFrame:
    if args.source:
        # Use text, not inferred JSON: malformed records must survive into bronze/quarantine.
        source = to_spark_path(args.source)
        reader = spark.readStream.option("maxFilesPerTrigger", args.max_files_per_trigger)
        path = spark._jvm.org.apache.hadoop.fs.Path(source)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if fs.exists(path) and fs.isFile(path):
            # Spark's file stream requires a directory basePath even for a single file.
            reader = reader.option("pathGlobFilter", path.getName())
            source = path.getParent().toString()
        raw = reader.text(source)
        ms = F.get_json_object("value", "$.ingest_ts").cast("long")
        bounds = Trade.model_json_schema()["properties"]["ingest_ts"]
        return raw.select(
            F.get_json_object("value", "$.symbol").alias("key"),
            "value",
            F.lit(topic).alias("topic"),
            F.lit(0).cast("int").alias("partition"),
            F.lit(None).cast("long").alias("offset"),  # file input has no Kafka offsets
            F.coalesce(
                F.when(ms.between(bounds["minimum"], bounds["exclusiveMaximum"] - 1), F.timestamp_millis(ms)),
                F.current_timestamp(),
            ).alias("timestamp"),
        )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("kafka.security.protocol", settings.kafka_security_protocol)
        .option("subscribe", topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .load()
    )


def bronze_records(df: DataFrame) -> DataFrame:
    """Preserve raw UTF-8 values and Kafka lineage, including malformed payloads."""
    return df.select(
        F.col("key").cast("string"),
        F.col("value").cast("string"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.lit("kafka").alias("source"),
        F.date_format("timestamp", "yyyy-MM-dd").alias(contracts.PARTITION_DATE),
        F.hour("timestamp").alias(contracts.PARTITION_HOUR),
    ).select(*contracts.BRONZE_COLUMNS)


def checkpoint(settings: Settings, job: str, query: str) -> str:
    return f"{settings.checkpoint_root.rstrip('/')}/{job}/{query}"


def parquet_writer(df: DataFrame, path: str, partitions: tuple[str, ...] = ()) -> DataStreamWriter:
    writer = df.writeStream.format("parquet").option("path", path).option("compression", "zstd")
    return writer.partitionBy(*partitions) if partitions else writer


def start_query(
    writer: DataStreamWriter,
    settings: Settings,
    job: str,
    name: str,
    args: argparse.Namespace,
    mode: str = "append",
) -> StreamingQuery:
    writer = (
        writer.outputMode(mode)
        .queryName(f"{job}_{name.replace('.', '_')}")
        .option("checkpointLocation", checkpoint(settings, job, name))
    )
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime=args.trigger)
    return writer.start()


def wait_queries(queries: list[StreamingQuery]) -> None:
    """Surface a failure in any owned query and stop its siblings in the caller's finally block."""
    logged: dict[str, int] = {}

    def progress(query: StreamingQuery) -> None:
        for batch in query.recentProgress:
            key = str(query.id)
            if batch["batchId"] <= logged.get(key, -1):
                continue
            logged[key] = batch["batchId"]
            state = batch.get("stateOperators", [])
            LOG.info(
                "streaming_batch",
                extra={
                    "query": query.name,
                    "batch_id": batch["batchId"],
                    "input_rows": batch["numInputRows"],
                    "watermark": batch.get("eventTime", {}).get("watermark"),
                    "rows_dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in state),
                    "state_rows": sum(op.get("numRowsTotal", 0) for op in state),
                    "duration_ms": batch.get("durationMs", {}),
                },
            )

    while any(q.isActive for q in queries):
        for query in queries:
            if query.exception() is not None:
                raise query.exception()
            progress(query)
            if query.isActive:
                query.awaitTermination(1)
    for query in queries:
        if query.exception() is not None:
            raise query.exception()
        progress(query)


def stop_queries(queries: list[StreamingQuery]) -> None:
    for query in reversed(queries):
        if query.isActive:
            query.stop()


def preview_sink(path: str) -> Callable[[DataFrame, int], None]:
    """Inspectable update batches for laptop tests; retries replace the same batch directory.

    This is an explicit alternative to Cassandra, not a table of finalized candles. Read all batches and
    select the greatest batch_id per key to reconstruct the latest upsert view.
    """

    def write(df: DataFrame, batch_id: int) -> None:
        df.write.mode("overwrite").parquet(f"{path}/batch_id={batch_id}")

    return write
