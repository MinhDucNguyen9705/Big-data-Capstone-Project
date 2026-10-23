# Spark processing

Scope: transforms, batch jobs, joins, UDFs, query plans. Main contributors: C, D. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-22: sortWithinPartitions lost before partitionBy write (Spark 3.4+ planned write)

* **Context / symptom:** `batch_silver` wrote `repartition("date").sortWithinPartitions("symbol", "event_ts")` with
  `partitionBy("date")`, but the Parquet file of a day was not sorted (test
  `test_silver_files_are_sorted_by_symbol_and_time` failed at the second row).
* **What we tried:** checked one file per date (yes, one) and the row order inside it (scrambled).
* **Numbers:** 492 rows, first out-of-order pair at index 1.
* **Fix / decision:** sort by `date, symbol, event_ts`. Since Spark 3.4 the V1 writer requires an ordering on the
  partition columns; when the child ordering does not start with them it adds its own `Sort(date)`, which is not
  stable and discards the earlier sort. With `date` as the prefix the requirement is met and no sort is added.
* **Lesson:** when writing partitioned files, put the partition columns first in any sort you want to keep, and
  test file-level order, not just DataFrame contents.
