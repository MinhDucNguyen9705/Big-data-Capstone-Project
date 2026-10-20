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
