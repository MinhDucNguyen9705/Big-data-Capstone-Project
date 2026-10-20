-- Serving layer schema (CONTRACT, see docs/contracts.md). Owner: E. Applied by the cassandra-schema Job.
-- Table names match cryptopipe.contracts.CASSANDRA_*. Local: SimpleStrategy RF=1; cloud: NetworkTopologyStrategy RF=3.

CREATE KEYSPACE IF NOT EXISTS crypto
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 1-minute candles from the speed layer (stream_ohlcv, owner C).
-- One partition per symbol per UTC day (1440 rows x exchanges); newest first, so "last N candles" is one slice.
-- Upsert by primary key: replaying a micro-batch rewrites the same rows (idempotent).
CREATE TABLE IF NOT EXISTS crypto.ohlcv_1m (
  symbol        text,
  day           date,
  window_start  timestamp,
  exchange      text,
  window_end    timestamp,
  open          double,
  high          double,
  low           double,
  close         double,
  volume        double,
  quote_volume  double,
  trades        bigint,
  vwap          double,
  updated_at    timestamp,
  PRIMARY KEY ((symbol, day), window_start, exchange)
) WITH CLUSTERING ORDER BY (window_start DESC, exchange ASC)
  AND default_time_to_live = 2592000;  -- 30 days of hot data; history lives in HDFS

-- Last traded price per symbol and exchange (speed layer, owner C).
-- Written with USING TIMESTAMP = event time in microseconds, so an older event never overwrites a newer one.
CREATE TABLE IF NOT EXISTS crypto.latest_price (
  symbol    text,
  exchange  text,
  price     double,
  qty       double,
  event_ts  timestamp,
  PRIMARY KEY (symbol, exchange)
);

-- Alerts from stateful detection (B3, owner C; read by E). alert_id is deterministic, so replays upsert.
CREATE TABLE IF NOT EXISTS crypto.alerts (
  symbol     text,
  day        date,
  event_ts   timestamp,
  alert_id   text,
  type       text,
  severity   text,
  value      double,
  threshold  double,
  details    map<text, text>,
  PRIMARY KEY ((symbol, day), event_ts, alert_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, alert_id ASC)
  AND default_time_to_live = 7776000;  -- 90 days

-- Daily batch view (B2, owner D; read by E): one row per symbol per UTC day, recomputed from silver.
CREATE TABLE IF NOT EXISTS crypto.daily_stats (
  symbol         text,
  day            date,
  open           double,
  high           double,
  low            double,
  close          double,
  volume         double,
  quote_volume   double,
  trades         bigint,
  vwap           double,
  log_return     double,
  volatility_1m  double,  -- stddev of 1m log returns over the day
  updated_at     timestamp,
  PRIMARY KEY (symbol, day)
) WITH CLUSTERING ORDER BY (day DESC);
