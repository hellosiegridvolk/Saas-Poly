"""Pure position + balance math (spec §3.2, §3.5).

These functions are stateless; the engine wires them to the repositories.
They handle the four cases produced by a fill:

  ===========  =================================  ===================
  prior size   fill side                          outcome
  ===========  =================================  ===================
  +long        buy                                grow long; cost-avg
  +long        sell <= size                       reduce long; realize
  +long        sell  > size                       flip to short
  -short       sell                               grow short; cost-avg
  -short       buy <= |size|                      reduce short; realize
  -short       buy  > |size|                      flip to long
  0            buy                                open long
  0            sell                               open short
  ===========  =================================  ===================
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

ZERO = Decimal(0)


@dataclass(frozen=True)
class PositionUpdate:
    new_size: Decimal
    new_average_cost: Decimal
    realized_pnl_delta: Decimal


def apply_fill_to_position(
    *,
    prior_size: Decimal,
    prior_avg_cost: Decimal,
    fill_side: Literal["buy", "sell"],
    fill_size: Decimal,
    fill_price: Decimal,
) -> PositionUpdate:
    if fill_size <= 0:
        raise ValueError("fill_size must be positive")

    signed_fill = fill_size if fill_side == "buy" else -fill_size

    # Same side (or opening): cost-average; no realized PnL.
    if prior_size == 0 or (prior_size > 0 and signed_fill > 0) or (prior_size < 0 and signed_fill < 0):
        new_size = prior_size + signed_fill
        # Weighted average against absolute notionals
        prior_notional = abs(prior_size) * prior_avg_cost
        fill_notional = fill_size * fill_price
        new_avg = (
            (prior_notional + fill_notional) / abs(new_size) if new_size != 0 else ZERO
        )
        return PositionUpdate(
            new_size=new_size, new_average_cost=new_avg, realized_pnl_delta=ZERO
        )

    # Opposite side: reduce (and possibly flip).
    close_size = min(abs(signed_fill), abs(prior_size))
    direction = Decimal(1) if prior_size > 0 else Decimal(-1)
    realized_per_share = (fill_price - prior_avg_cost) * direction
    realized = realized_per_share * close_size

    new_size = prior_size + signed_fill
    if new_size == 0:
        return PositionUpdate(
            new_size=ZERO, new_average_cost=ZERO, realized_pnl_delta=realized
        )
    if (prior_size > 0) == (new_size > 0):
        # Did not flip: avg cost preserved on the remainder.
        return PositionUpdate(
            new_size=new_size,
            new_average_cost=prior_avg_cost,
            realized_pnl_delta=realized,
        )
    # Flipped: residual opens a new position at fill price.
    return PositionUpdate(
        new_size=new_size,
        new_average_cost=fill_price,
        realized_pnl_delta=realized,
    )


def apply_fill_to_balance(
    *,
    prior_balance: Decimal,
    fill_side: Literal["buy", "sell"],
    fill_size: Decimal,
    fill_price: Decimal,
    fee: Decimal,
) -> Decimal:
    notional = fill_size * fill_price
    if fill_side == "buy":
        return prior_balance - notional - fee
    return prior_balance + notional - fee
