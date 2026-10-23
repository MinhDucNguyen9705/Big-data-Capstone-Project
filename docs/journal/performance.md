# Performance

Scope: benchmarks with numbers: caching, bucketing, AQE, partition pruning. Main contributors: D. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-22: OOM in batch_silver from caching the raw JSON column (local mode)

* **Context / symptom:** `batch_silver --local` on 5.3 M real trades: `java.lang.OutOfMemoryError: Java heap
  space` in `UnsafeSorterSpillReader` during the dedupe aggregation (local[*], default 1 GB heap, ~10 tasks).
* **What we tried:** the job persisted the parsed DataFrame (which keeps the `raw` JSON string, ~200 B/row) only
  to count input rows, so about 1 GB of cache competed with execution memory for the sort.
* **Numbers:** before: OOM after ~20 s. After caching only the silver output (typed columns): the run completes
  in 146.6 s with the same 1 GB heap (trades + klines + bucketed table).
* **Fix / decision:** count the input without caching (column pruning makes `count()` cheap) and persist only
  the DataFrame that is written twice (`jobs/batch_silver.py::run_kind`).
* **Lesson:** cache the smallest DataFrame that is reused, never one carrying wide raw payloads "for counts".
