"""Always-fresh tick-size lookup + price alignment helpers.

Polymarket can change tick size intraday when a market becomes one-sided
(Nautilus #2980). Order build path therefore *always* refreshes the tick
size right before constructing the order. Read-only paths (UI/metrics)
use a TTL cache to avoid hammering the CLOB.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Literal, Protocol

Side = Literal["BUY", "SELL"]


class _TickSizeReader(Protocol):
    async def get_tick_size(self, token_id: str) -> str: ...


def align_price_to_tick(
    price: Decimal, tick: Decimal, *, side: Side
) -> Decimal:
    """Round `price` to the nearest `tick` in the safe direction for `side`.

    BUY → ROUND_DOWN  (don't pay more than intended)
    SELL → ROUND_UP   (don't accept less than intended)
    """
    if side == "BUY":
        rounding = ROUND_DOWN
    elif side == "SELL":
        rounding = ROUND_UP
    else:
        msg = f"side must be 'BUY' or 'SELL', got {side!r}"
        raise ValueError(msg)
    quantized = (price / tick).quantize(Decimal("1"), rounding=rounding)
    return quantized * tick


@dataclass
class _CacheEntry:
    value: Decimal
    fetched_at: float


class TickSizeCache:
    """Per-token tick-size cache with TTL + force-refresh.

    Used by:
      - Execution Agent's order_builder (force=True before each order build)
      - Read-only consumers (force=False; respects ttl_s)
    """

    def __init__(self, client: _TickSizeReader, ttl_s: float = 60.0) -> None:
        self._client = client
        self._ttl_s = ttl_s
        self._cache: dict[str, _CacheEntry] = {}

    async def get(self, token_id: str, *, force: bool = False) -> Decimal:
        if not force and self._ttl_s > 0:
            entry = self._cache.get(token_id)
            if entry is not None:
                if (time.monotonic() - entry.fetched_at) < self._ttl_s:
                    return entry.value
        raw = await self._client.get_tick_size(token_id)
        tick = Decimal(str(raw))
        self._cache[token_id] = _CacheEntry(value=tick, fetched_at=time.monotonic())
        return tick
