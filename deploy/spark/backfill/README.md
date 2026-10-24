# deploy/spark/backfill/

Owner: **D (Batch and data)**. One-off jobs that load history into the lake. They are **not** applied by
`make deploy WITH_JOBS=1` (that only picks up `deploy/spark/batch-*.yaml`, here the hourly `batch-silver.yaml`):
apply them by hand, in order, when you want to (re)load data.

Prerequisites: `make cluster-up build-images deploy` (A), so namespace `spark`, the operator, ConfigMaps
`cryptopipe-env` / `hadoop-conf`, images `cryptopipe-spark` / `cryptopipe-ingest` and the lake folders
(`hdfs-init-lake`, including `/lake/raw/archive`) exist.

| Step | File | What it does | Output |
|---|---|---|---|
| 1 | `archive-download-job.yaml` | `crypto backfill download` into a staging volume, then `hdfs dfs -put` | `/lake/raw/archive/<kind>/<SYMBOL>/*.csv` |
| 2 | `load-archive.yaml` | `batch_load_archive --kind trades` and `--kind klines` | `/lake/bronze/archive_trades`, `/lake/bronze/archive_klines` |
| 3 | `silver-rebuild.yaml` | `batch_silver` over every bronze date | `/lake/silver/*`, `/lake/quarantine/{archive_trades,klines}`, `silver.trades_bucketed` |
| 4 | `profile.yaml` | `profile --detail` | driver log (Markdown for `docs/data.md`), `/lake/_profile/*` |

```bash
# 1. once per range: edit SYMBOLS/KIND/START/END in the Job (both containers' KIND), then
kubectl -n spark delete job archive-download --ignore-not-found
kubectl apply -f deploy/spark/backfill/archive-download-job.yaml
kubectl -n spark wait --for=condition=complete job/archive-download --timeout=2h
kubectl -n spark logs job/archive-download -c put

# 2-4. Spark applications (delete the previous object first: an application runs once per apply)
for app in load-archive silver-rebuild profile; do
  kubectl delete -f deploy/spark/backfill/$app.yaml --ignore-not-found
  kubectl apply -f deploy/spark/backfill/$app.yaml
  # wait until `kubectl -n spark get sparkapplications` shows COMPLETED before the next step
done
kubectl -n spark logs batch-profile-driver          # paste the Markdown into docs/data.md

# check
kubectl -n spark exec -it <any pod with hdfs> -- hdfs dfs -ls /lake/silver/trades
```

Notes:

* **Sizing.** The checkpoint dataset (30 days of trades for 10 symbols, about 15-20 GB of CSV, plus 2 years of
  1m klines, about 2 GB) does not fit one staging volume on a laptop: run the download Job per 3-5 trade days.
  Every step is idempotent, so a range can be downloaded and loaded again safely.
* **Memory.** Each Spark application here uses 1 driver (1 GB) and 1 executor (2 GB). On the laptop `kind`
  cluster run them one at a time and, for a big load, pause the streaming applications (C) first.
* **Bucketed table.** `silver-rebuild.yaml` and `../batch-silver.yaml` set `spark.sql.warehouse.dir` to
  `hdfs://hdfs-namenode.storage:8020/user/spark/warehouse`, so `silver.trades_bucketed` survives the pod.
* **Quarantine.** `batch_silver` writes invalid archive trades to `quarantine/archive_trades` and never touches
  `quarantine/trades`, which is the streaming file sink of `stream_ohlcv` (C).
