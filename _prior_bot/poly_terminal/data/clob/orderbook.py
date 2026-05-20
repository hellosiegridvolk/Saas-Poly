"""Pure-function order-book helpers.

The CLOB returns books as `{bids: [{price,size}], asks: [{price,size}]}`.
This module:
  - parses raw responses into a typed `BookSnapshot`;
  - exposes pure helpers (imbalance, depth, spread, gap) used by both the
    Orderbook Intelligence Agent and the Risk Agent's slippage gate.

No network code; the agent that owns the WebSocket subscription feeds
snapshots in.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

Side = Literal["bid", "ask"]


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class BookSnapshot:
    token_id: str
    bids: list[BookLevel] = field(default_factory=list)  # best (highest) first
    asks: list[BookLevel] = field(default_factory=list)  # best (lowest) first
    ts: int = 0

    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None


def parse_clob_book(raw: dict[str, Any]) -> BookSnapshot:
    """Parse a Polymarket CLOB book response into a typed snapshot.

    Bids returned descending by price, asks ascending — independent of the
    server's ordering, so callers always get best-first.
    """
    bids_raw = raw.get("bids") or []
    asks_raw = raw.get("asks") or []
    bids = sorted(
        (BookLevel(price=Decimal(str(b["price"])), size=Decimal(str(b["size"])))
         for b in bids_raw),
        key=lambda l: l.price,
        reverse=True,
    )
    asks = sorted(
        (BookLevel(price=Decimal(str(a["price"])), size=Decimal(str(a["size"])))
         for a in asks_raw),
        key=lambda l: l.price,
    )
    ts = int(raw.get("timestamp", 0))
    return BookSnapshot(
        token_id=str(raw.get("asset_id", raw.get("token_id", ""))),
        bids=bids,
        asks=asks,
        ts=ts,
    )


def spread_cents(book: BookSnapshot) -> Decimal | None:
    bb = book.best_bid()
    ba = book.best_ask()
    if bb is None or ba is None:
        return None
    return (ba - bb) * Decimal("100")


def bid_ask_imbalance(book: BookSnapshot, top_n: int = 5) -> Decimal | None:
    """`(bid_size - ask_size) / (bid_size + ask_size)` over top_n levels each side.

    Range [-1, 1]. Positive = bids deeper (buy pressure). Returns None on
    empty book or zero total depth.
    """
    bid_size = sum((l.size for l in book.bids[:top_n]), start=Decimal("0"))
    ask_size = sum((l.size for l in book.asks[:top_n]), start=Decimal("0"))
    total = bid_size + ask_size
    if total == 0:
        return None
    return (bid_size - ask_size) / total


def depth_top_n_usd(book: BookSnapshot, side: Side, top_n: int = 5) -> Decimal:
    """Sum of `price * size` across the top N levels on `side`."""
    if side == "bid":
        levels = book.bids[:top_n]
    elif side == "ask":
        levels = book.asks[:top_n]
    else:
        msg = f"side must be 'bid' or 'ask', got {side!r}"
        raise ValueError(msg)
    return sum((l.price * l.size for l in levels), start=Decimal("0"))


def liquidity_gap_ticks(
    book: BookSnapshot,
    side: Side,
    min_size_usd: Decimal | int,
    tick_size: Decimal,
) -> int | None:
    """Return the number of ticks between best and the first deep-enough level.

    Used by the Orderbook Intelligence Agent's gap signal: a wide gap means
    the visible top-of-book is thin and a market sweep would cross into a
    much worse fill.
    """
    if side == "bid":
        levels = book.bids
    elif side == "ask":
        levels = book.asks
    else:
        msg = f"side must be 'bid' or 'ask', got {side!r}"
        raise ValueError(msg)
    if not levels:
        return None
    threshold = Decimal(str(min_size_usd))
    best_price = levels[0].price
    for level in levels:
        if level.price * level.size >= threshold:
            diff = abs(best_price - level.price)
            return int(round(diff / tick_size))
    return None
