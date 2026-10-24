#!/usr/bin/env bash
# Owner: E. Schema readiness does not imply that live producers/Spark are running.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"
require_tools kubectl
k -n storage rollout status statefulset/cassandra --timeout=60s
for table in ohlcv_1m latest_price alerts daily_stats; do
  k -n storage exec cassandra-0 -- cqlsh -e "SELECT * FROM crypto.$table LIMIT 1;" >/dev/null
  ok "Cassandra table crypto.$table is queryable"
done
if [[ "${REQUIRE_LIVE:-0}" == 1 ]]; then
  rows="$(k -n storage exec cassandra-0 -- cqlsh -e 'SELECT symbol FROM crypto.latest_price LIMIT 1;')"
  [[ "$rows" != *"(0 rows)"* ]] || die "latest_price is empty: run ingestion and stream_ohlcv first"
  rows="$(k -n storage exec cassandra-0 -- cqlsh -e 'SELECT symbol FROM crypto.ohlcv_1m LIMIT 1;')"
  [[ "$rows" != *"(0 rows)"* ]] || die "ohlcv_1m is empty: wait for a streaming micro-batch"
  ok "Cassandra contains serving rows (verify provenance separately if demo data was seeded)"
fi
