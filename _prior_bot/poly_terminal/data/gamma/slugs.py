"""Polymarket Gamma slug builders — Bug #4 root fix.

The 5m / 15m / 1h slug formats are DIFFERENT, despite all being short-window
crypto Up/Down markets. v2 ported the 1h format to the 15m path and
discovered zero markets for 30 hours. v3 has separate, tested builders.

Verified examples (April 2026):
  5m :  btc-updown-5m-1771168800            (handiko/Polymarket-Market-Finder)
  15m:  btc-updown-15m-1768502700           (py-clob-client#244)
  1h :  bitcoin-up-or-down-april-18-2026-7am-et    (Polymarket frontend)
"""

from __future__ import annotations

from datetime import datetime
from typing import Final
from zoneinfo import ZoneInfo

_SHORT_WINDOW_ASSETS: Final[frozenset[str]] = frozenset({"btc", "eth", "sol", "xrp"})

# 1h slugs use the full asset name, not the ticker.
_LONG_NAME: Final[dict[str, str]] = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "ripple",
}

_ET = ZoneInfo("America/New_York")


def _validate_asset(asset: str) -> str:
    asset_l = asset.lower()
    if asset_l not in _SHORT_WINDOW_ASSETS:
        msg = (
            f"unsupported short-window asset: {asset!r} "
            f"(expected one of {sorted(_SHORT_WINDOW_ASSETS)})"
        )
        raise ValueError(msg)
    return asset_l


def build_5m_slug(asset: str, ts: int) -> str:
    """5-minute slug. `ts` is floored to the previous 300-second boundary."""
    asset_l = _validate_asset(asset)
    window_start = (ts // 300) * 300
    return f"{asset_l}-updown-5m-{window_start}"


def build_15m_slug(asset: str, ts: int) -> str:
    """15-minute slug. `ts` is floored to the previous 900-second boundary."""
    asset_l = _validate_asset(asset)
    window_start = (ts // 900) * 900
    return f"{asset_l}-updown-15m-{window_start}"


def build_1h_slug(asset: str, dt: datetime) -> str:
    """1-hour slug — human-readable ET timestamp.

    `dt` must be timezone-aware. Any timezone is accepted; the slug is
    always rendered in America/New_York (ET) per Polymarket's convention.
    """
    if dt.tzinfo is None:
        msg = "build_1h_slug requires a timezone-aware datetime"
        raise ValueError(msg)
    asset_l = _validate_asset(asset)
    et = dt.astimezone(_ET)
    name = _LONG_NAME[asset_l]
    month = et.strftime("%B").lower()
    day = et.day
    year = et.year
    hour_12 = et.hour % 12 or 12
    ampm = "am" if et.hour < 12 else "pm"
    return f"{name}-up-or-down-{month}-{day}-{year}-{hour_12}{ampm}-et"
