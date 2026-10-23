# ADR 0003: HDFS (not MinIO / S3) for the data lake

* Status: accepted (2026-10-22)
* Deciders: A (Platform), C and D (lake writers)
* Related: [contracts.md](../contracts.md) section 4, [deploy/hdfs/](../../deploy/hdfs/), ADR 0002,
  [journal/storage.md](../journal/storage.md)

## Context

The lake holds bronze/silver/gold Parquet and the Structured Streaming checkpoints (`CRYPTO_LAKE_ROOT`,
`CRYPTO_CHECKPOINT_ROOT`). The course is about big-data storage and processing, and its rubric covers the
storage layer's replication, fault tolerance and scaling. Streaming checkpoints need atomic renames and
consistent listings. Everything runs on Kubernetes (ADR 0002), on laptops first.

## Options considered

1. **HDFS on Kubernetes** (`apache/hadoop:3.3.6`, NameNode + DataNode StatefulSets): the reference distributed
   file system; block replication, rack/node placement, NameNode safe mode and DataNode re-replication can be
   demonstrated and measured. Spark 3.5 ships the matching Hadoop 3.3 client, so no extra jars. Harder to run on
   Kubernetes: DataNode identity and DNS, formatting rules, no official Helm chart we trust.
2. **MinIO (S3 API)**: one StatefulSet, easy UI, cloud-like. But `s3a://` needs `hadoop-aws` + AWS SDK jars,
   rename is copy+delete (streaming checkpoints need the S3A committers and care), and the storage concepts the
   course examines (blocks, replication factor, NameNode metadata) are hidden.
3. **Local PVC / hostPath Parquet**: trivial, but single node, no replication, not shareable across executors on
   different nodes.

## Decision

HDFS 3.3.6 on Kubernetes, with: a ClusterIP Service for the NameNode (stable address across pod restarts),
DataNodes registering by stable DNS names (`hdfs-datanode-<i>.hdfs-datanode.storage.svc.cluster.local`,
`dfs.client.use.datanode.hostname=true`), format only when `VERSION` is missing, lake folders created from
`contracts.LAKE_PATHS` by a Job. MinIO stays the documented fallback (`CRYPTO_LAKE_ROOT=s3a://lake`, plan section 4)
if HDFS blocks the team; it changes only the ConfigMap and the Spark image.

## Consequences

* + Real replication and failure drills (Q3): kill a DataNode, measure re-replication; kill the NameNode, check
  no re-format and no data loss.
* + No extra jars; the History Server reads event logs from the same HDFS.
* - Single NameNode (no HA) locally: a NameNode restart pauses all lake I/O for ~1 min (safe mode).
* - Laptop clients outside the cluster cannot reach DataNodes directly (their names resolve only in-cluster); local
  Spark against the cluster lake needs to run in a pod, or use the sample SparkApplication.
* Follow-ups: NameNode HA with JournalNodes on the cloud cluster if time allows; HDFS snapshots for backups (Q3).
