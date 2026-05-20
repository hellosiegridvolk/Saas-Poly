"""One-intent-per-bar debouncer.

Used by scalp + flash strategies to ensure RSI/MACD flip-flops within a
single bar don't produce duplicate intents.
"""

from __future__ import annotations


class OneIntentPerBar:
    """Emit at most one intent per (asset, bar_start_ts)."""

    def __init__(self) -> None:
        self._seen: set[tuple[str, int]] = set()

    def should_emit(self, asset: str, bar_start_ts: int) -> bool:
        key = (asset, bar_start_ts)
        if key in self._seen:
            return False
        self._seen.add(key)
        return True

    def prune_older_than(self, cutoff_ts: int) -> None:
        self._seen = {(a, t) for (a, t) in self._seen if t >= cutoff_ts}
