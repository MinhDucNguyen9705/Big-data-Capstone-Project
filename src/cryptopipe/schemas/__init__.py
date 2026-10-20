"""Message contracts: one pydantic model per Kafka topic. Spark jobs use `to_spark_schema(Model)`."""

from cryptopipe.schemas.alert import Alert
from cryptopipe.schemas.asset_meta import AssetMeta
from cryptopipe.schemas.common import SCHEMA_VERSION, DlqRecord, to_spark_schema
from cryptopipe.schemas.fear_greed import FearGreed
from cryptopipe.schemas.kline import Kline
from cryptopipe.schemas.trade import Trade

__all__ = [
    "SCHEMA_VERSION",
    "Alert",
    "AssetMeta",
    "DlqRecord",
    "FearGreed",
    "Kline",
    "Trade",
    "to_spark_schema",
]
