# Ingestion

Scope: WebSocket producers, REST pollers, archive downloads, DLQ. Main contributors: B. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-23: Coinbase `side` is the maker side, not the taker side

* **Context / symptom:** the `Trade` contract needs the taker side (Binance gives `m` = buyer is maker, so
  `m=true` -> `sell`). Coinbase's Advanced Trade `market_trades` sends `side: BUY|SELL` and the docs don't say
  whose side it is. Mapping it straight across would flip every Coinbase buy/sell volume and imbalance metric.
* **What we tried:** polled the public `GET /api/v3/brokerage/market/products/BTC-USD/ticker?limit=8` three
  times and compared each trade price with `best_bid` / `best_ask` in the same response.
* **Numbers:** bid 81604.81 / ask 81604.82: all 4 `BUY` printed at 81604.81 (the bid), all 4 `SELL` at 81604.82
  (the ask). A second sample shows 5 `BUY`s with the same timestamp walking down 81595.97 -> 81591.19, i.e. one
  aggressive seller sweeping the bids.
* **Fix / decision:** `coinbase_ws.TAKER_SIDE` inverts it (`BUY` -> `sell`); test
  `test_side_is_inverted_to_taker` in `tests/unit/ingest/test_coinbase_ws.py`.
* **Lesson:** "side" means different things on different exchanges; check it against the order book before
  normalizing, and pin the result with a test.

## 2026-10-23: Binance gap backfill after reconnect

* **Context / symptom:** WebSocket reconnects (24 h server close, network errors) lose the trades sent while
  disconnected.
* **What we tried:** `aggTrades?fromId=` is keyless but its ids are aggregate-trade ids, not the `t` trade ids
  of the `@trade` stream, so it can't resume from the last trade id. `historicalTrades` works without an API key
  now (checked 2026-10-23) and takes the trade id as `fromId`.
* **Numbers:** a manual backfill from `last_id - 2500` on BTCUSDT fetched exactly 2500 trades in 3 pages
  (weight 25 per call). Cap: 5 pages (5000 trades) per symbol per reconnect; longer gaps are left to the archive.
* **Fix / decision:** `binance_ws._backfill` runs on every reconnect, from the last seen id per symbol; overlap
  with the new stream is absorbed by downstream dedupe on `(exchange, symbol, trade_id)`. Metric
  `crypto_ingest_gap_trades_backfilled`.
* **Lesson:** resume from a cursor the stream itself gives you (trade id), and let idempotent downstream dedupe
  handle the overlap rather than trying to make the handover exact.
