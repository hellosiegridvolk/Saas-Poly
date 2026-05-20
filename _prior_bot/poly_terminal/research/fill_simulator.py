"""Pure-function order-book fill simulator for offline backtesting.

No I/O, no clock, no logging — every function is deterministic given inputs.
Used by execution_replay to walk historical L2 ladders and produce realistic
fill outcomes (filled_usd, filled_shares, avg_price, partial flag, reject
reason). The escalator function emulates the production sell-side price-walk
strategy that takes successive passes at progressively worse prices.

A "level" is a dict {'price': float, 'size': float}. Asks are walked
ascending (lowest price first); bids are walked descending (highest price
first). Inputs are sorted defensively — callers MUST NOT be trusted to
pre-sort the book.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FillResult:
    """Outcome of a single FAK (fill-and-kill) order against a static book.

    - filled_usd: USD value of shares actually consumed (sum of price*size)
    - filled_shares: total shares consumed
    - avg_price: weighted average fill price (None when no fill)
    - partial: True when some but not all of the requested quantity filled
    - reject_reason: free-form code for total-reject cases
        - 'empty_book': levels list was empty
        - 'no_match': all levels were beyond worst_price (no executable depth)
    - levels_consumed: count of price levels that contributed (>=1 implies
      at least a partial fill)
    """

    filled_usd: float = 0.0
    filled_shares: float = 0.0
    avg_price: float | None = None
    partial: bool = False
    reject_reason: str | None = None
    levels_consumed: int = 0


def _coerce_levels(levels: list[dict]) -> list[tuple[float, float]]:
    """Convert raw level dicts to (price, size) tuples, dropping garbage."""
    out: list[tuple[float, float]] = []
    for lvl in levels:
        try:
            price = float(lvl["price"])
            size = float(lvl["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if size <= 0 or price <= 0:
            continue
        out.append((price, size))
    return out


def simulate_fak_buy(
    asks: list[dict], usd_amount: float, worst_price: float
) -> FillResult:
    """Simulate a fill-and-kill BUY against the given asks.

    Walks asks in ascending price order. At each level, takes as many shares
    as fit in the remaining USD budget, but stops the moment a level's price
    exceeds `worst_price`. Returns a FillResult.

    Edge cases:
      - asks empty → reject_reason='empty_book'
      - all asks have price > worst_price → reject_reason='no_match'
      - book runs out before USD budget exhausted → partial=True
    """
    if not asks:
        return FillResult(reject_reason="empty_book")
    if usd_amount <= 0:
        return FillResult(reject_reason="empty_book")

    sorted_asks = sorted(_coerce_levels(asks), key=lambda lv: lv[0])
    if not sorted_asks:
        return FillResult(reject_reason="empty_book")

    remaining_usd = float(usd_amount)
    total_filled_usd = 0.0
    total_filled_shares = 0.0
    levels_consumed = 0

    for price, size in sorted_asks:
        if price > worst_price:
            break
        if remaining_usd <= 0:
            break

        max_usd_at_level = price * size
        if remaining_usd >= max_usd_at_level:
            # Take the whole level.
            total_filled_usd += max_usd_at_level
            total_filled_shares += size
            remaining_usd -= max_usd_at_level
            levels_consumed += 1
        else:
            # Partial-take at this level: split by USD budget.
            shares_taken = remaining_usd / price
            total_filled_usd += remaining_usd
            total_filled_shares += shares_taken
            remaining_usd = 0.0
            levels_consumed += 1
            break

    if levels_consumed == 0:
        # Either all asks were above worst_price, or zero usable depth.
        return FillResult(reject_reason="no_match")

    avg_price = total_filled_usd / total_filled_shares if total_filled_shares > 0 else None
    partial = remaining_usd > 1e-9  # USD epsilon

    return FillResult(
        filled_usd=total_filled_usd,
        filled_shares=total_filled_shares,
        avg_price=avg_price,
        partial=partial,
        reject_reason=None,
        levels_consumed=levels_consumed,
    )


def simulate_fak_sell(
    bids: list[dict], shares: float, worst_price: float
) -> FillResult:
    """Simulate a fill-and-kill SELL against the given bids.

    Walks bids in descending price order. At each level, sells as many shares
    as remain in the request, but stops the moment a level's price falls
    below `worst_price`. Returns a FillResult.

    Edge cases:
      - bids empty → reject_reason='empty_book'
      - all bids have price < worst_price → reject_reason='no_match'
      - book runs out before share quantity hit → partial=True
    """
    if not bids:
        return FillResult(reject_reason="empty_book")
    if shares <= 0:
        return FillResult(reject_reason="empty_book")

    sorted_bids = sorted(_coerce_levels(bids), key=lambda lv: lv[0], reverse=True)
    if not sorted_bids:
        return FillResult(reject_reason="empty_book")

    remaining_shares = float(shares)
    total_filled_usd = 0.0
    total_filled_shares = 0.0
    levels_consumed = 0

    for price, size in sorted_bids:
        if price < worst_price:
            break
        if remaining_shares <= 0:
            break

        if remaining_shares >= size:
            # Take the whole level.
            total_filled_usd += price * size
            total_filled_shares += size
            remaining_shares -= size
            levels_consumed += 1
        else:
            # Partial-take.
            total_filled_usd += price * remaining_shares
            total_filled_shares += remaining_shares
            remaining_shares = 0.0
            levels_consumed += 1
            break

    if levels_consumed == 0:
        return FillResult(reject_reason="no_match")

    avg_price = total_filled_usd / total_filled_shares if total_filled_shares > 0 else None
    partial = remaining_shares > 1e-9

    return FillResult(
        filled_usd=total_filled_usd,
        filled_shares=total_filled_shares,
        avg_price=avg_price,
        partial=partial,
        reject_reason=None,
        levels_consumed=levels_consumed,
    )


def simulate_sell_escalator(
    bids: list[dict],
    shares: float,
    initial_price: float,
    max_attempts: int = 3,
    undercut_pct: float = 0.02,
    min_price: float = 0.05,
) -> dict:
    """Production-style escalator: try to sell `shares` at `initial_price`.

    On a `no_match` (no bid at-or-above current floor), drop the floor by
    `undercut_pct` and retry. Floor is clipped at `min_price`. Stops as soon
    as the full `shares` quantity is filled, or `max_attempts` are exhausted,
    or the floor would drop below `min_price`.

    Returns:
        {
            "total_filled_shares": float,
            "avg_price": float | None,
            "attempts_used": int,
            "exhausted": bool,        # True iff shares not fully filled
            "prices_tried": list[float],
        }
    """
    remaining_shares = float(shares)
    total_filled_usd = 0.0
    total_filled_shares = 0.0
    attempts_used = 0
    prices_tried: list[float] = []
    current_price = float(initial_price)

    # Track the residual book across attempts so consumed depth at attempt
    # N isn't re-offered at attempt N+1. Sort once, descending.
    residual: list[list[float]] = [
        [p, s] for (p, s) in sorted(_coerce_levels(bids), key=lambda lv: lv[0], reverse=True)
    ]

    while attempts_used < max_attempts and remaining_shares > 1e-9:
        # Clip price floor.
        attempt_floor = max(current_price, min_price)
        prices_tried.append(attempt_floor)
        attempts_used += 1

        # Build a snapshot of the residual book as level-dicts.
        snapshot = [{"price": p, "size": s} for (p, s) in residual if s > 0]
        result = simulate_fak_sell(snapshot, remaining_shares, attempt_floor)

        if result.filled_shares > 0:
            total_filled_usd += result.filled_usd
            total_filled_shares += result.filled_shares
            remaining_shares -= result.filled_shares

            # Drain residual: same algorithm as simulate_fak_sell, mutating in place.
            to_consume = result.filled_shares
            for level in residual:
                if to_consume <= 1e-9:
                    break
                if level[1] <= 0 or level[0] < attempt_floor:
                    continue
                if level[1] <= to_consume:
                    to_consume -= level[1]
                    level[1] = 0.0
                else:
                    level[1] -= to_consume
                    to_consume = 0.0

        if remaining_shares <= 1e-9:
            break

        # Step down for next attempt.
        next_price = current_price * (1.0 - undercut_pct)
        if next_price < min_price:
            # If we already tried at min_price (because attempt_floor was clipped
            # equal to min_price), there's no point retrying at the same floor.
            if attempt_floor <= min_price + 1e-12:
                break
            next_price = min_price
        current_price = next_price

    avg_price = total_filled_usd / total_filled_shares if total_filled_shares > 0 else None
    exhausted = remaining_shares > 1e-9

    return {
        "total_filled_shares": total_filled_shares,
        "avg_price": avg_price,
        "attempts_used": attempts_used,
        "exhausted": exhausted,
        "prices_tried": prices_tried,
    }
