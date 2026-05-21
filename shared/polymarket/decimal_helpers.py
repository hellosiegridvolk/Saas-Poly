"""Money-math primitives. Pure functions; safe to import anywhere.

Encodes the spec invariants from §3.2, §3.8, §11.2:

- Prices quantize to 0.001 (one mill), rounded DOWN.
- Sizes quantize to 0.000001 (one micro-share), rounded DOWN.
- USDC from the SDK arrives as integer 6-decimal units; divide by 1_000_000
  exactly once at the boundary.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal

PRICE_TICK: Decimal = Decimal("0.001")
SIZE_TICK: Decimal = Decimal("0.000001")
USDC_SCALE: Decimal = Decimal(1_000_000)


def quantize_price(value: Decimal | int | str) -> Decimal:
    """Round a price down to the CLOB tick. Never rounds up into a worse fill."""
    return Decimal(value).quantize(PRICE_TICK, rounding=ROUND_DOWN)


def quantize_size(value: Decimal | int | str) -> Decimal:
    """Round a share size down to the CLOB tick. Never rounds up into oversize."""
    return Decimal(value).quantize(SIZE_TICK, rounding=ROUND_DOWN)


def usdc_from_raw(raw: int) -> Decimal:
    """Convert a raw 6-decimal USDC integer (SDK boundary) to a Decimal balance."""
    return Decimal(raw) / USDC_SCALE
