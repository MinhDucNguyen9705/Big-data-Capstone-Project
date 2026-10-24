# Quality and testing

Scope: data quality rules, quarantine, unit and integration tests. Main contributors: C, D. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-20: collected timestamps come back in the driver's local time zone

* **Context / symptom:** candle tests failed with `window_start=2025-10-10 06:55` vs expected `23:55`. The laptop is
  UTC+7; Spark computed in UTC (`spark.sql.session.timeZone=UTC`) but PySpark converts `TimestampType` to naive
  local `datetime` on `collect()`.
* **Fix / decision:** tests build expected rows with timezone-aware UTC datetimes (`datetime(..., tzinfo=UTC)` or
  `fromisoformat("...Z")`) and compare DataFrames with chispa, or format timestamps inside Spark
  (`date_format`) before collecting. Never compare collected naive datetimes with hand-written ones.
* **Lesson:** keep time comparisons inside Spark or make both sides timezone-aware.

## 2026-10-24: a live candle query must include the previous UTC date after midnight

* **Context / symptom:** Cassandra partitions candles by `(symbol, day)`, so querying only today's day would
  omit the start of a Last 30 minutes view immediately after UTC midnight.
* **What we tried:** tested 07:05 Vietnam time (00:05 UTC) with a range starting 30 minutes earlier.
* **Numbers:** expected partitions are 2026-10-23 and 2026-10-24. The HTTP probe rejects ranges over 24 hours,
  which cannot generally be covered by just the start/end date partitions.
* **Fix / decision:** the dashboard uses a bounded `day IN (range-start UTC date, range-end UTC date)` query.
  Cassandra derives these dates from epoch-ms macros with `toDate((timestamp) ...)`, avoiding browser-local
  Grafana date formatting. The probe test checks UTC+7 input; a real Cassandra test verifies both partitions.
* **Lesson:** dashboard timezone and storage partition timezone are part of query correctness.

## 2026-10-24: upstream CRD YAML contains a scalar PyYAML handles differently

* **Context / symptom:** extracting OpenAPI schemas from the pinned Prometheus CRDs failed with
  `could not determine a constructor for the tag tag:yaml.org,2002:value` on an unquoted `=` enum item.
* **What we tried:** kept the original chart untouched and added a SafeLoader subclass that handles that
  scalar tag as text. Built-in Kubernetes resources still use kubeconform's official strict schemas.
* **Numbers:** the final check validated 80 resources with no skipped custom resources.
* **Fix / decision:** the handling is local to the validation script's loader; no unsafe YAML loader or
  ignore-missing-schemas fallback is used.
* **Lesson:** validate the actual dependency bundle and account for YAML parser differences explicitly.
