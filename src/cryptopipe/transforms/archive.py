# Owner: D (Batch and data) | Task: A6 (batch_load_archive) | Status: implemented
"""Binance archive CSV -> contract columns, so archive history and live stream share one silver schema.

Archive rows go to bronze as JSON of the contract model (`Trade` / `Kline`) with `topic = "archive"`,
`partition = -1` and `offset` = row number in the file (bronze columns, see contracts.BRONZE_COLUMNS), so `batch_silver`
parses stream and archive bronze the same way.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from cryptopipe.contracts import BRONZE_COLUMNS, PARTITION_DATE, PARTITION_HOUR
from cryptopipe.schemas import SCHEMA_VERSION, Kline, Trade, to_spark_schema
from cryptopipe.symbols import load_symbols

# Headerless trade archive CSV columns
ARCHIVE_TRADES_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("price", DoubleType()),
        StructField("qty", DoubleType()),
        StructField("quote_qty", DoubleType()),
        StructField("time", LongType()),  # ms before 2025, microseconds from 2025-01-01
        StructField("is_buyer_maker", BooleanType()),
        StructField("is_best_match", BooleanType()),
    ]
)

# Headerless kline archive CSV columns (spot, e.g. BTCUSDT-1m-2025-09.csv)
ARCHIVE_KLINES_SCHEMA = StructType(
    [
        StructField("open_time", LongType()),  # ms before 2025, microseconds from 2025-01-01
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("close_time", LongType()),
        StructField("quote_volume", DoubleType()),
        StructField("trades", LongType()),
        StructField("taker_buy_volume", DoubleType()),
        StructField("taker_buy_quote_volume", DoubleType()),
        StructField("ignore", StringType()),
    ]
)

TRADE_COLUMNS = to_spark_schema(Trade).fieldNames()
KLINE_COLUMNS = to_spark_schema(Kline).fieldNames()
ARCHIVE_TOPIC = "archive"  # bronze `topic` of archive rows
SOURCE_FILE = "source_file"  # lineage column added by `with_file_offsets`

# Magnitude thresholds: below 1e11 is seconds (until year 5138), below 1e14 ms, below 1e17 us, else ns.
_S_MAX, _MS_MAX, _US_MAX = 10**11, 10**14, 10**17


def epoch_to_ms(col_name: str) -> Column:
    """Column expression normalizing an epoch in s/ms/us/ns to ms, detected by magnitude."""
    c = F.col(col_name).cast("long")
    return (
        F.when(c < _S_MAX, c * 1000)
        .when(c < _MS_MAX, c)
        .when(c < _US_MAX, F.expr(f"`{col_name}` div 1000"))
        .otherwise(F.expr(f"`{col_name}` div 1000000"))
        .cast("long")
    )


def _symbol_col(symbol: str | Column) -> Column:
    """Canonical symbol literal (validated against conf/symbols.yaml), or a Column computed by the caller."""
    if isinstance(symbol, Column):
        return symbol
    if symbol not in load_symbols():
        raise ValueError(f"unknown canonical symbol {symbol!r} (not in the symbols file)")
    return F.lit(symbol)


def _extra_columns(df: DataFrame, schema: StructType) -> list[str]:
    return [c for c in df.columns if c not in schema.fieldNames()]


def archive_trades_to_contract(df_csv: DataFrame, symbol: str | Column) -> DataFrame:
    """Archive trades (ARCHIVE_TRADES_SCHEMA) for canonical `symbol` -> `Trade` columns.

    exchange = "binance", trade_id = string(id), side = "sell" if is_buyer_maker else "buy",
    event_ts = time normalized us -> ms, ingest_ts = load time, schema_version = 1.
    `symbol` may also be a Column (one symbol per file, see `batch_load_archive`). Input columns that are not
    archive columns (lineage such as `source_file`, `offset`) are kept after the `Trade` columns.
    """
    return df_csv.select(
        F.lit("binance").alias("exchange"),
        _symbol_col(symbol).alias("symbol"),
        F.col("id").cast("string").alias("trade_id"),
        F.col("price"),
        F.col("qty"),
        F.when(F.col("is_buyer_maker"), F.lit("sell"))
        .when(~F.col("is_buyer_maker"), F.lit("buy"))
        .alias("side"),  # null flag -> null side -> quarantined as malformed in silver
        epoch_to_ms("time").alias("event_ts"),
        F.expr("unix_millis(current_timestamp())").alias("ingest_ts"),
        F.lit(SCHEMA_VERSION).cast("long").alias("schema_version"),
        *_extra_columns(df_csv, ARCHIVE_TRADES_SCHEMA),
    )


def archive_klines_to_contract(df_csv: DataFrame, symbol: str | Column, interval: str = "1m") -> DataFrame:
    """Archive klines (ARCHIVE_KLINES_SCHEMA) -> `Kline` columns. Archive candles are closed (`is_closed=True`).

    open_ts/close_ts are normalized to ms like trades. Extra input columns are kept, as in
    `archive_trades_to_contract`.
    """
    return df_csv.select(
        F.lit("binance").alias("exchange"),
        _symbol_col(symbol).alias("symbol"),
        F.lit(interval).alias("interval"),
        epoch_to_ms("open_time").alias("open_ts"),
        epoch_to_ms("close_time").alias("close_ts"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades",
        F.lit(True).alias("is_closed"),
        F.lit(SCHEMA_VERSION).cast("long").alias("schema_version"),
        *_extra_columns(df_csv, ARCHIVE_KLINES_SCHEMA),
    )


def with_file_offsets(df_csv: DataFrame, order_by: str) -> DataFrame:
    """Add `source_file` (input file URI) and `offset` (0-based row number in that file, ordered by `order_by`).

    Must be applied directly on the CSV read, before any shuffle (`input_file_name()` is empty afterwards).
    Archive files are sorted by trade id / open time, so the row number equals the line number.
    """
    with_file = df_csv.withColumn(SOURCE_FILE, F.input_file_name())
    window = Window.partitionBy(SOURCE_FILE).orderBy(order_by)
    return with_file.withColumn("offset", (F.row_number().over(window) - 1).cast("long"))


def contract_to_bronze(df: DataFrame, value_columns: list[str], ts_col: str) -> DataFrame:
    """Contract rows (+ `offset`) -> `contracts.BRONZE_COLUMNS` for archive bronze.

    key = symbol, value = JSON of `value_columns`, topic = "archive", partition = -1, source = "archive".
    `kafka_ts` (and so `date`/`hour`) is the event time `ts_col`: a rerun of the same files rewrites the same
    date partitions. Rows whose time did not parse fall back to the load time and are quarantined by silver.
    """
    kafka_ts = F.coalesce(F.timestamp_millis(F.col(ts_col)), F.current_timestamp())
    return df.select(
        F.col("symbol").alias("key"),
        F.to_json(F.struct(*value_columns)).alias("value"),
        F.lit(ARCHIVE_TOPIC).alias("topic"),
        F.lit(-1).alias("partition"),
        F.col("offset").cast("long").alias("offset"),
        kafka_ts.alias("kafka_ts"),
        F.lit("archive").alias("source"),
    ).select(
        *BRONZE_COLUMNS[:-2],
        F.date_format("kafka_ts", "yyyy-MM-dd").alias(PARTITION_DATE),
        F.hour("kafka_ts").alias(PARTITION_HOUR),
    )
