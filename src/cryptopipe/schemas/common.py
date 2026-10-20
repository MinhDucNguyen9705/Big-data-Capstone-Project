"""Base model, shared validators and the pydantic -> Spark schema helper used by every contract model."""

from __future__ import annotations

import time
import types
import typing
from typing import Annotated, Any, Literal, Union, get_args, get_origin

from pydantic import AfterValidator, BaseModel, ConfigDict, Field
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

SCHEMA_VERSION = 1
EXCHANGES = ("binance", "coinbase")

# Epoch milliseconds (UTC). The bounds (2009-01-01 .. year 2286) catch seconds/microseconds mix-ups.
EpochMs = Annotated[int, Field(ge=1_230_768_000_000, lt=10_000_000_000_000)]
PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class ContractModel(BaseModel):
    """Base for every Kafka message. Unknown fields are rejected.

    Every subclass declares `schema_version: int = SCHEMA_VERSION` as its last field (kept out of the base class
    so it stays last in the JSON and in the Spark schema).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    def to_json(self) -> str:
        return self.model_dump_json()


def validate_canonical_symbol(value: str) -> str:
    from cryptopipe.symbols import load_symbols

    if value not in load_symbols():
        raise ValueError(f"unknown symbol {value!r} (not in the symbols file)")
    return value


# A canonical symbol (`BTC-USDT`) that is listed in `conf/symbols.yaml`.
CanonicalSymbol = Annotated[str, AfterValidator(validate_canonical_symbol)]


class DlqRecord(ContractModel):
    """Value of topic `dlq.ingest`: a message a producer could not validate, with the reason."""

    source: str  # e.g. "binance_ws", "coingecko"
    reason: str
    raw: str  # original payload, unchanged
    ingest_ts: EpochMs
    schema_version: int = SCHEMA_VERSION


# ---------------------------------------------------------------------------------------------------------
# pydantic -> Spark

_SCALARS: dict[type, DataType] = {
    str: StringType(),
    int: LongType(),
    float: DoubleType(),
    bool: BooleanType(),
}


def _spark_type(annotation: Any) -> tuple[DataType, bool]:
    """Return (Spark type, nullable) for a field annotation."""
    origin = get_origin(annotation)
    if origin is Annotated:
        return _spark_type(get_args(annotation)[0])
    if origin in (Union, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) != 1:
            raise TypeError(f"unsupported union {annotation}")
        return _spark_type(args[0])[0], True
    if origin is Literal:
        return _SCALARS[type(get_args(annotation)[0])], False
    if origin is list:
        return ArrayType(_spark_type(get_args(annotation)[0])[0], containsNull=False), False
    if origin is dict:
        key, value = get_args(annotation)
        return MapType(_spark_type(key)[0], _spark_type(value)[0], valueContainsNull=True), False
    if annotation in _SCALARS:
        return _SCALARS[annotation], False
    raise TypeError(f"no Spark type for {annotation!r}")


def to_spark_schema(model: type[BaseModel]) -> StructType:
    """Spark StructType with the model's fields, in declaration order.

    str -> string, int -> long (epoch ms fit), float -> double, bool -> boolean, list -> array, dict -> map,
    Literal -> the type of its values. `X | None` fields are nullable, all others are not.
    """
    hints = typing.get_type_hints(model, include_extras=True)
    fields = []
    for name in model.model_fields:
        dtype, nullable = _spark_type(hints[name])
        fields.append(StructField(name, dtype, nullable))
    return StructType(fields)
