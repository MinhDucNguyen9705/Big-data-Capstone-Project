# deploy/hdfs/

HDFS 3.3.6 (`apache/hadoop:3.3.6`, runs as uid 1000 `hadoop`, the HDFS superuser) for the data lake.
Owner: **A (Platform)**. Why HDFS: [ADR 0003](../../docs/adr/0003-hdfs-instead-of-minio.md).

```bash
kubectl apply -k deploy/hdfs/overlays/local      # done by make deploy (which also reruns the init-lake Job)
bash deploy/hdfs/check.sh                         # live DataNodes, 50 MB round trip, DataNode restart drill
bash deploy/hdfs/check.sh --namenode              # + NameNode restart drill (no re-format, data intact)
python deploy/hdfs/gen_lake_dirs.py               # regenerate base/lake-dirs.generated.txt from contracts.py
```

| File | What |
|---|---|
| `base/core-site.xml`, `base/hdfs-site.xml` | ConfigMap `hdfs-config`; copied to `spark` as `hadoop-conf` by `deploy-all.sh` |
| `base/namenode.yaml` | StatefulSet `hdfs-namenode` (1), PVC 10 Gi; formats only if `/data/namenode/current/VERSION` is missing |
| `base/datanode.yaml` | StatefulSet `hdfs-datanode` (2 local), PVC 20 Gi each; registers as `<pod>.hdfs-datanode.storage.svc.cluster.local` |
| `base/services.yaml` | `hdfs-namenode` (ClusterIP 8020/9870), `hdfs-namenode-hl` (headless), `hdfs-namenode-ui` (NodePort 30870), `hdfs-datanode` (headless) |
| `base/init-lake-job.yaml` | Job `hdfs-init-lake`: lake folders from `contracts.LAKE_PATHS`, `/spark-events`, `/user/spark`; owner `spark`, mode 775 |
| `overlays/{local,lite,cloud}` | DataNode count and `dfs.replication` per profile (`cloud` is a stub for Q2) |

**Rules that keep it working across restarts** (see `docs/journal/fault-tolerance.md`):

1. Every client uses exactly `hdfs://hdfs-namenode.storage:8020` (the contract value). A different authority, such
   as `...storage.svc`, is a different file system to Hadoop ("Wrong FS").
2. The NameNode Service has a stable ClusterIP, so DataNodes and clients survive a NameNode pod IP change.
3. DataNodes register by DNS name, and clients connect by name (`dfs.client.use.datanode.hostname=true`). Clients
   outside the cluster cannot resolve those names, so read and write the lake from pods (SparkApplications).
4. Never format by hand. A wiped NameNode PVC with DataNode PVCs left over gives `Incompatible clusterIDs`; delete
   the DataNode PVCs too (data loss, ask first) or restore the NameNode volume.

**Writing as another user:** folders belong to `spark` (the Spark image's user). For ad-hoc access from a pod, set
`HADOOP_USER_NAME=spark`, or run `hdfs` commands in `hdfs-namenode-0` (superuser):
`kubectl -n storage exec hdfs-namenode-0 -c namenode -- hdfs dfs -ls -R /lake`.
