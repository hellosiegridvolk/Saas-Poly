"""Polymarket Manual-Assist Scanner — pure logic.

Phase 41 (2026-05-12). Research telemetry tool: scans Polymarket
crypto Up/Down threshold bars in real-time and surfaces opportunities
matching the operator's manual-trading pattern (≥$80 spot-vs-threshold
buffer, <60s to bar close, current bid in [0.97, 0.98]).

This module is the PURE LOGIC — question parsing, buffer math, filter
evaluation. The CLI driver in `scripts/poly_manual_scanner.py` glues
this to live Polymarket + CoinGecko polling and the terminal output.

**This module does not trade.** It is operator decision support. The
trading bot's allocator and risk gates are untouched. The operator
sees opportunities and chooses whether to enter via the Polymarket UI.

Why a pure-logic module separate from the CLI:
  * Question-parsing is the hard part — regex + edge cases proliferate
    as Polymarket introduces new bar formats. Pinning every format in
    tests is cheap and high-value.
  * The filter math is easy to get wrong (direction sign, boundary
    inclusivity). Tests are the only reliable spec.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


# Asset name → canonical 3-letter ticker. Tested via parser tests.
_ASSET_PATTERNS: dict[str, str] = {
    r"\bbitcoin\b": "BTC",
    r"\bbtc\b": "BTC",
    r"\bethereum\b": "ETH",
    r"\beth\b": "ETH",
    r"\bether\b": "ETH",
    r"\bsolana\b": "SOL",
    r"\bsol\b": "SOL",
    r"\bxrp\b": "XRP",
    r"\bripple\b": "XRP",
}

# Direction keywords. Order matters — longer phrases first to win the
# alternation match before shorter substrings.
_DIRECTION_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bhigher than\b", re.IGNORECASE), "above"),
    (re.compile(r"\blower than\b", re.IGNORECASE), "below"),
    (re.compile(r"\babove\b", re.IGNORECASE), "above"),
    (re.compile(r"\bover\b", re.IGNORECASE), "above"),
    (re.compile(r"\bbelow\b", re.IGNORECASE), "below"),
    (re.compile(r"\bunder\b", re.IGNORECASE), "below"),
]

# Threshold price regex: captures e.g. "$80,000", "80000", "2,325",
# "2.50". Single pattern handles both comma-separated and bare
# numbers — start with digit, allow digits + commas, optional decimal.
# Tested against the "alternate_phrasings" + "above"/"below" cases.
_THRESHOLD_PATTERN = re.compile(
    r"\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)"
)

# "Up or Down" bars have NO threshold — they resolve on price movement
# during the bar window. Skip cleanly.
_UP_OR_DOWN_PATTERN = re.compile(r"up or down", re.IGNORECASE)


@dataclass(frozen=True)
class ThresholdMarket:
    """Parsed result of a threshold-bar question."""
    asset: str           # "BTC", "ETH", "SOL", "XRP"
    threshold_usd: float
    direction: str       # "above" or "below"


@dataclass(frozen=True)
class BarOpportunity:
    """One in-flight bar with all the data needed to evaluate the
    operator's filter."""
    question: str
    asset: str
    threshold_usd: float
    direction: str
    spot_usd: float
    dollar_buffer: float  # signed; positive = favorable to the YES side
    side: str             # "YES" or "NO" — which side the operator should buy
    bid: float
    ask: float
    time_to_close_s: int
    condition_id: str
    token_id: str


@dataclass(frozen=True)
class OperatorFilter:
    """Operator-tunable filter mirroring the manual-trading pattern.

    Defaults revised 2026-05-13:
      * `min_buffer_usd = 30` — lowered from $100 because at $100 the
        scanner returned 0 results across multiple checks (the strict
        window doesn't materialize that often). $30 is a "discovery"
        threshold for surfacing more candidates so the operator can
        decide which to actually pursue. Override with --min-buffer
        for stricter discipline.
      * `max_ttc_s = 60` — only enter in the final minute
      * `bid_lo = 0.97`, `bid_hi = 0.98` — quick-flip target zone
    """
    min_buffer_usd: float = 30.0
    max_ttc_s: int = 60
    bid_lo: float = 0.97
    bid_hi: float = 0.98


@dataclass(frozen=True)
class FilterDecision:
    accepted: bool
    reason: str


# ───────────────────────────── parser ─────────────────────────────
def parse_threshold_market(question: str | None) -> ThresholdMarket | None:
    """Parse 'Bitcoin above $80,000 on May 12, 2PM ET?' into structured
    fields. Returns None for any question that doesn't match the
    threshold-bar shape — caller skips those silently.

    Skipped shapes:
      * "Up or Down" bars (no explicit threshold)
      * Non-crypto markets
      * Malformed strings
    """
    if not question or not isinstance(question, str):
        return None
    if _UP_OR_DOWN_PATTERN.search(question):
        return None
    # Asset detection
    asset = None
    for pat, ticker in _ASSET_PATTERNS.items():
        if re.search(pat, question, re.IGNORECASE):
            asset = ticker
            break
    if asset is None:
        return None
    # Direction detection
    direction = None
    for pat, dir_str in _DIRECTION_PATTERNS:
        if pat.search(question):
            direction = dir_str
            break
    if direction is None:
        return None
    # Threshold extraction — find a numeric value after the direction
    # word (or first numeric in the question if direction is ambiguous).
    # Simpler: first numeric is the threshold for these question shapes.
    match = _THRESHOLD_PATTERN.search(question)
    if not match:
        return None
    raw = match.group(1).replace(",", "")
    try:
        threshold = float(raw)
    except ValueError:
        return None
    # Sanity check — single-digit thresholds are likely date/time
    # fragments, not prices. BTC/ETH/SOL/XRP thresholds are always ≥1.
    if threshold < 1.0 and asset != "XRP":
        return None
    return ThresholdMarket(
        asset=asset, threshold_usd=threshold, direction=direction,
    )


# ──────────────────────────── filter ──────────────────────────────
def apply_filter(
    opp: BarOpportunity, flt: OperatorFilter,
) -> FilterDecision:
    """Evaluate the operator filter against one opportunity. Returns
    accepted=True iff every gate passes."""
    if opp.dollar_buffer < flt.min_buffer_usd:
        return FilterDecision(
            accepted=False,
            reason=(
                f"buffer ${opp.dollar_buffer:.2f} < "
                f"min ${flt.min_buffer_usd:.2f}"
            ),
        )
    if opp.time_to_close_s > flt.max_ttc_s:
        return FilterDecision(
            accepted=False,
            reason=f"ttc {opp.time_to_close_s}s > max {flt.max_ttc_s}s",
        )
    # Boundary inclusive [lo, hi] — picked because operator's pattern
    # explicitly mentions both 0.97 and 0.98 as targets.
    if not (flt.bid_lo <= opp.bid <= flt.bid_hi):
        return FilterDecision(
            accepted=False,
            reason=(
                f"bid {opp.bid:.4f} outside "
                f"[{flt.bid_lo:.4f}, {flt.bid_hi:.4f}]"
            ),
        )
    return FilterDecision(
        accepted=True,
        reason=(
            f"OK: buffer=${opp.dollar_buffer:.2f}, ttc={opp.time_to_close_s}s, "
            f"bid={opp.bid:.4f}"
        ),
    )


def compute_buffer_usd(
    *, spot_usd: float, threshold_usd: float, direction: str,
) -> tuple[float, str]:
    """Return (signed_buffer_usd, favored_side).

    For 'above' bars: positive buffer when spot > threshold → YES side
    is favored (price needs to STAY above to win).
    For 'below' bars: positive buffer when spot < threshold → YES side
    is favored (price needs to STAY below to win).

    A POSITIVE buffer is the operator's edge — there's room for spot
    to drift before the bar flips against them.
    """
    if direction == "above":
        buffer = spot_usd - threshold_usd
    elif direction == "below":
        buffer = threshold_usd - spot_usd
    else:
        raise ValueError(f"unknown direction {direction!r}")
    side = "YES" if buffer > 0 else "NO"
    return float(buffer), side
