# Cassandra serving storage

Owner **Chi / E**, A7. Single-node local checkpoint configuration, Cassandra **5.0.6**, 10 Gi PVC,
1 Gi JVM heap, 1536 Mi requested RAM / 2560 Mi limit. Namespace `storage`, client Service
`cassandra.storage:9042`; these names match the existing contracts and Spark writers.

From the repo root, after A's cluster exists:

```bash
make install
bash deploy/cassandra/install.sh
bash deploy/cassandra/check.sh
REQUIRE_LIVE=1 bash deploy/cassandra/check.sh  # requires rows, not just an existing schema
```

The existing `make deploy` calls `install.sh` automatically. It applies the StatefulSet/Services, waits for
CQL readiness, generates the schema ConfigMap **from `cql/schema.cql`**, and reruns the disposable schema Job.
Reinstalling does not delete the StatefulSet or PVC. The schema uses `IF NOT EXISTS`; changing an existing
table needs an explicit reviewed migration, not just another install.

`PROFILE=lite` uses the same one-node Cassandra. Scripts follow the platform's `CLUSTER_NAME` and
`KUBE_CONTEXT` settings; the default context is `kind-crypto`. Cloud deployment is intentionally not supported
by this local RF=1 / unauthenticated checkpoint configuration.

## Inspect and troubleshoot

```bash
kubectl --context kind-crypto -n storage get pods,pvc,svc
kubectl --context kind-crypto -n storage logs cassandra-0
kubectl --context kind-crypto -n storage logs job/cassandra-schema
kubectl --context kind-crypto -n storage exec cassandra-0 -- cqlsh -e \
  "SELECT * FROM crypto.latest_price WHERE symbol='BTC-USDT' AND exchange='binance';"
```

Pending PVC: check the default StorageClass and free disk. Slow first boot: the startup probe allows 15 minutes;
inspect CQL/logs before changing probe limits. OOMKilled: leave room outside the 1 Gi heap for native/off-heap
memory. Healthy empty tables: start B's producers and C's `stream_ohlcv`; schema readiness is a different check.

## Independent synthetic demo

Generate data into **`crypto_demo` only**, on explicit request. The generator prints CQL and never opens a
connection itself. It creates 60 recent BTC-USDT/Binance candles plus a latest price; it leaves `crypto` alone.

```bash
.venv/bin/python -m cryptopipe.serving.seed_demo > /tmp/crypto-serving-demo.cql
kubectl --context kind-crypto -n storage exec -i cassandra-0 -- cqlsh < /tmp/crypto-serving-demo.cql
```

Choose **crypto_demo = SYNTHETIC** in the Grafana Dataset selector. This verifies rendering and queries;
it does not establish that ingestion/streaming are working. The live verification command rejects this dataset.

Full walkthrough: [../../docs/getting-started.md](../../docs/getting-started.md).
