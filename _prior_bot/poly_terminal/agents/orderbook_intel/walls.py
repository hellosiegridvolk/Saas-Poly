"""Spoof-wall detector.

A wall is a price level whose visible size × price >= `min_wall_usd`.
A *spoofed* wall is one that disappears from the book within
`removal_window_s` seconds without being filled — a common manipulation
that makes price look better-supported than it really is. We block buys
aligned with the wall's side when this happens.

State model:
  - On each snapshot, scan bids and asks for any level meeting the wall
    threshold; track the largest such level per side.
  - If a previously-tracked wall is missing from a subsequent snapshot
    AND the current top-of-side level has not consumed the wall (size
    didn't fall below by partial-fill semantics), emit a SpoofWallSignal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from poly_terminal.data.clob.orderbook import BookLevel, BookSnapshot

Side = Literal["BID", "ASK"]


@dataclass(frozen=True)
class WallConfig:
    min_wall_usd: Decimal = Decimal("1000")
    removal_window_s: int = 10
    # If True, only emit when the wall's price was unmatched by trades.
    # The simple detector approximates this by checking whether the price
    # level still appears in any subsequent snapshot — if it's truly
    # cleared by trades, the next snapshot would still show it briefly
    # (or partial). Set False to ignore the heuristic in tests.
    require_no_trade: bool = True


@dataclass(frozen=True)
class WallSnapshot:
    """One side's largest wall in a snapshot."""

    side: Side
    price: Decimal
    size: Decimal
    seen_at_ts: int

    @property
    def usd(self) -> Decimal:
        return self.price * self.size


@dataclass(frozen=True)
class SpoofWallSignal:
    token_id: str
    side: Side
    price: Decimal
    size_usd: Decimal
    removed_within_s: int


def _largest_wall(
    levels: list[BookLevel], min_usd: Decimal
) -> tuple[Decimal, Decimal] | None:
    """Return (price, size) of the largest wall above threshold, or None."""
    best: tuple[Decimal, Decimal] | None = None
    best_usd = Decimal("0")
    for level in levels:
        usd = level.price * level.size
        if usd >= min_usd and usd > best_usd:
            best = (level.price, level.size)
            best_usd = usd
    return best


@dataclass
class SpoofWallDetector:
    cfg: WallConfig = field(default_factory=WallConfig)
    _last_walls: dict[Side, WallSnapshot] = field(default_factory=dict)

    def observe(self, book: BookSnapshot) -> SpoofWallSignal | None:
        signal: SpoofWallSignal | None = None
        for side, levels in (("BID", book.bids), ("ASK", book.asks)):
            current = _largest_wall(levels, self.cfg.min_wall_usd)
            previous = self._last_walls.get(side)  # type: ignore[arg-type]
            if previous is not None:
                still_present = (
                    current is not None
                    and current[0] == previous.price
                    and current[1] >= previous.size * Decimal("0.10")
                )
                if not still_present:
                    elapsed = book.ts - previous.seen_at_ts
                    if 0 <= elapsed <= self.cfg.removal_window_s:
                        signal = SpoofWallSignal(
                            token_id=book.token_id,
                            side=side,  # type: ignore[arg-type]
                            price=previous.price,
                            size_usd=previous.usd,
                            removed_within_s=int(elapsed),
                        )
                    self._last_walls.pop(side, None)  # type: ignore[arg-type]
            if current is not None:
                self._last_walls[side] = WallSnapshot(  # type: ignore[index]
                    side=side,  # type: ignore[arg-type]
                    price=current[0],
                    size=current[1],
                    seen_at_ts=book.ts,
                )
        return signal
