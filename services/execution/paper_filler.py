"""Paper-mode fill simulator.

Walks a single-level book snapshot for the signal's side and produces a
:class:`PaperFillResult` that the engine turns into a Fill. Decimal math
throughout, ``ROUND_DOWN`` quantization at the boundaries (spec §3.2,
§11.2).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from shared.polymarket import quantize_price, quantize_size


@dataclass(frozen=True)
class BookSnapshot:
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_depth_at_best: Decimal
    ask_depth_at_best: Decimal


@dataclass(frozen=True)
class PaperFillResult:
    filled_size: Decimal
    fill_price: Decimal
    fee: Decimal


PAPER_FEE_PCT: Decimal = Decimal("0.02")


def simulate_fill(
    *,
    side: Literal["buy", "sell"],
    size: Decimal,
    limit_price: Decimal,
    book: BookSnapshot,
    fee_pct: Decimal = PAPER_FEE_PCT,
) -> PaperFillResult | None:
    """Return a partial-or-full fill against the top of book, or ``None``
    if the marketable side is empty or limit price misses it entirely.

    Paper-mode semantics are conservative: we fill at the best price on
    the opposite side, capped by depth at that level. Limit price must
    be marketable (>= best_ask for buys, <= best_bid for sells)."""

    if side == "buy":
        if book.best_ask is None:
            return None
        if limit_price < book.best_ask:
            return None
        fill_price = book.best_ask
        available = book.ask_depth_at_best
    else:
        if book.best_bid is None:
            return None
        if limit_price > book.best_bid:
            return None
        fill_price = book.best_bid
        available = book.bid_depth_at_best

    filled = quantize_size(min(size, available))
    if filled <= 0:
        return None

    fill_price = quantize_price(fill_price)
    notional = filled * fill_price
    fee = quantize_size(notional * fee_pct)
    return PaperFillResult(filled_size=filled, fill_price=fill_price, fee=fee)
