# Ingestion

Scope: WebSocket producers, REST pollers, archive downloads, DLQ. Main contributors: B. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-22: Binance archive timestamps are microseconds since 2025

* **Context / symptom:** `BTCUSDT-trades-2025-09-01.csv` has 16-digit times (`1756684800178247`). Read as ms they
  land in the year 57637; the `Trade` model (`EpochMs` bounds) rejects them, Spark `timestamp_millis` overflows.
* **What we tried:** a fixed `/ 1000` breaks pre-2025 files (ms); converting by file date couples the code to
  Binance's cut-over date.
* **Numbers:** 2 days BTCUSDT = 5,325,712 trades, all with 16-digit times; 1m klines (open/close time) same.
* **Fix / decision:** normalize by magnitude at the boundary (`< 1e11` s, `< 1e14` ms, `< 1e17` us, else ns):
  `transforms/archive.py::epoch_to_ms` (Spark, integer `div`, no double rounding) and `ingest/replay.py::to_ms`.
  Tests cover all four units.
* **Lesson:** convert units where data enters the system and detect them from the value, not from metadata
  that can change again.
