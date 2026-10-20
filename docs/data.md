# Data

Owner: **D (Batch and data)**. First version in task A4, real counts and sizes in task A8.

To fill in:

* Sources: Binance WebSocket, Coinbase WebSocket, CoinGecko, Fear & Greed, data.binance.vision archive (URLs,
  formats, rate limits). Layout, topics and tables are in [contracts.md](contracts.md).
* Timestamp units: Binance spot archive files use **microseconds** from 2025-01-01 (milliseconds before);
  normalized to ms at load time (see `tests/fixtures/archive_trades_sample.csv`).
* Checkpoint dataset: symbols, date ranges, number of files, CSV size, Parquet size, row counts per symbol/day
  (paste the output of `crypto job run profile`).
* Live data accumulated: messages per topic per day.
