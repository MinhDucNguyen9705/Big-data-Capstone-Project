"""Canonical symbol mapping (`BTC-USDT` <-> `BTCUSDT` / `BTC-USD` / `bitcoin`), loaded from `conf/symbols.yaml`."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

EXCHANGE_KEYS = ("binance", "coinbase", "coingecko")
_REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SymbolInfo:
    canonical: str
    binance: str | None = None
    coinbase: str | None = None
    coingecko: str | None = None


class SymbolMap:
    """Exchange <-> canonical lookups. `exchange` is one of `binance`, `coinbase`, `coingecko`."""

    def __init__(self, entries: list[SymbolInfo]):
        self.entries = entries
        self._by_canonical = {e.canonical: e for e in entries}
        self._to_canonical: dict[str, dict[str, str]] = {k: {} for k in EXCHANGE_KEYS}
        for e in entries:
            for key in EXCHANGE_KEYS:
                name = getattr(e, key)
                if name:
                    self._to_canonical[key][name.upper()] = e.canonical

    @classmethod
    def from_yaml(cls, path: str | Path) -> SymbolMap:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls([SymbolInfo(**item) for item in data["symbols"]])

    @property
    def canonicals(self) -> list[str]:
        return [e.canonical for e in self.entries]

    def __contains__(self, canonical: object) -> bool:
        return canonical in self._by_canonical

    def to_canonical(self, exchange: str, exchange_symbol: str) -> str | None:
        """`to_canonical("binance", "btcusdt") -> "BTC-USDT"`; None if the symbol is not tracked."""
        return self._to_canonical[exchange].get(exchange_symbol.upper())

    def to_exchange(self, exchange: str, canonical: str) -> str | None:
        """`to_exchange("coinbase", "BTC-USDT") -> "BTC-USD"`; None if not listed on that exchange."""
        info = self._by_canonical.get(canonical)
        return getattr(info, exchange) if info else None

    def for_exchange(self, exchange: str) -> list[str]:
        """Exchange-specific names of every symbol listed on `exchange`, in config order."""
        return [getattr(e, exchange) for e in self.entries if getattr(e, exchange)]


def _resolve(path: str) -> Path:
    p = Path(path)
    if p.is_absolute() or p.exists():
        return p
    return _REPO_ROOT / p  # running from another cwd inside a source checkout


@lru_cache(maxsize=4)
def load_symbols(path: str | None = None) -> SymbolMap:
    """Cached SymbolMap for `path`, default `settings.symbols_file`."""
    if path is None:
        from cryptopipe.config import settings

        path = settings.symbols_file
    return SymbolMap.from_yaml(_resolve(path))
