"""Gate 11: simulate fill against current book; reject above slippage cap."""

from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable

from poly_terminal.data.clob.orderbook import BookSnapshot
from poly_terminal.shared.typed_reject import Reject

BookReader = Callable[[str], Awaitable[BookSnapshot | None]]


def _simulate_fill(
    book: BookSnapshot, side: str, size_usd: Decimal
) -> Decimal | None:
    """Return weighted-avg fill price walking the offered side, or None if
    the book can't absorb size_usd."""
    levels = book.asks if side == "BUY" else book.bids
    if not levels:
        return None
    remaining = size_usd
    cost = Decimal("0")
    shares = Decimal("0")
    for level in levels:
        avail_usd = level.price * level.size
        if remaining <= avail_usd:
            partial_shares = remaining / level.price
            cost += partial_shares * level.price
            shares += partial_shares
            remaining = Decimal("0")
            break
        cost += level.size * level.price
        shares += level.size
        remaining -= avail_usd
    if remaining > 0:
        return None
    return cost / shares if shares > 0 else None


class SlippageSimulationGate:
    def __init__(
        self,
        max_bps: Decimal,
        book_reader: BookReader,
    ) -> None:
        self._max_bps = max_bps
        self._read = book_reader

    async def __call__(self, intent: object) -> Reject | None:
        token_id = str(getattr(intent, "token_id", ""))
        side = str(getattr(intent, "side", "BUY"))
        side_str = side.value if hasattr(side, "value") else str(side)
        size_usd = Decimal(str(getattr(intent, "size_usd", 0)))
        limit_price = Decimal(str(getattr(intent, "limit_price", 0)))

        book = await self._read(token_id)
        if book is None:
            return Reject(code="book_unavailable")

        avg_fill = _simulate_fill(book, side_str, size_usd)
        if avg_fill is None:
            return Reject(code="book_too_thin", detail=str(size_usd))

        if limit_price <= 0:
            return Reject(code="invalid_limit_price")

        # Slippage = |avg_fill - limit_price| / limit_price * 10_000 bps.
        slippage_bps = (
            abs(avg_fill - limit_price) / limit_price * Decimal(10_000)
        )
        if slippage_bps > self._max_bps:
            return Reject(
                code="slippage_above_cap",
                detail=f"{slippage_bps:.0f}bps > {self._max_bps}bps",
            )
        return None
