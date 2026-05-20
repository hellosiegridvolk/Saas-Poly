"""Confidence source — abstraction for `verified_true_p` lookups.

The Endgame Yield family asks: "for this market+token, what's our
externally-verified probability the outcome resolves in our favor?"
This module defines the protocol; the simplest production binding
is `ManualConfidenceSource` (operator-curated dict, populated from
env or a runtime API).

Future sources plug into the same protocol:
  * external oracle feed (Chainlink / signed data)
  * multi-source verifier (cross-checks N independent feeds)
  * model-driven (TradingView signal, sports API, weather API)

The strategy treats `found=False` as "no confidence — skip" so a
missing entry never auto-promotes to a trade.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfidenceQuery:
    market_id: str
    token_id: str


@dataclass(frozen=True)
class ConfidenceResult:
    found: bool
    true_p: float
    sources_count: int


class ConfidenceSource(Protocol):
    """Anything that can answer "what's our confidence in this token?".

    Implementations must be cheap (no network I/O on the hot path);
    operator-curated dicts and cached oracle reads are the canonical
    shapes.
    """

    def lookup(self, query: ConfidenceQuery) -> ConfidenceResult:
        ...


class ManualConfidenceSource:
    """Operator-curated `dict[(market_id, token_id), (true_p, sources_count)]`.

    Populated via `set(...)` (programmatic) or `load_from_env_string(...)`
    (env-driven). `lookup` returns `found=False` for any unset key —
    callers must skip the trade rather than guess.
    """

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], tuple[float, int]] = {}

    def set(
        self, market_id: str, token_id: str, *,
        true_p: float, sources_count: int,
    ) -> None:
        if not (0.0 <= true_p <= 1.0):
            raise ValueError(
                f"true_p must be in [0,1], got {true_p!r}"
            )
        if sources_count < 0:
            raise ValueError(
                f"sources_count must be >= 0, got {sources_count!r}"
            )
        self._entries[(market_id, token_id)] = (
            float(true_p), int(sources_count)
        )

    def clear(self, market_id: str, token_id: str) -> None:
        self._entries.pop((market_id, token_id), None)

    def lookup(self, query: ConfidenceQuery) -> ConfidenceResult:
        key = (query.market_id, query.token_id)
        if key not in self._entries:
            return ConfidenceResult(found=False, true_p=0.0, sources_count=0)
        true_p, sources = self._entries[key]
        return ConfidenceResult(
            found=True, true_p=true_p, sources_count=sources,
        )

    def find_market_id_for_token(self, token_id: str) -> str | None:
        """Reverse-lookup: given a token_id, return the market_id of
        the matching override entry, or None if no entry matches.

        Added 2026-05-12 for certainty_farm's tick-driven flow —
        EVT_MARKET_TICK payloads carry token_id but not market_id, so
        the strategy needs this lookup to find the right market for
        the evaluator. O(n) over entries; acceptable because n is
        operator-curated (~1-10 entries in practice).
        """
        for (mid, tid), _ in self._entries.items():
            if tid == token_id:
                return mid
        return None

    def load_from_env_string(self, raw: str) -> None:
        """Parse `m1:t1:0.97:2,m2:t2:0.92:3` and set entries.

        Best-effort: malformed rows are logged + skipped; good rows
        in the same string are still applied. Whitespace and empty
        rows tolerated.
        """
        if not raw:
            return
        for chunk in raw.split(","):
            row = chunk.strip()
            if not row:
                continue
            parts = row.split(":")
            if len(parts) != 4:
                logger.warning(
                    "ManualConfidenceSource: malformed row %r "
                    "(want m:t:p:n) — skipping", row,
                )
                continue
            mid, tid, p_str, n_str = parts
            try:
                p = float(p_str)
                n = int(n_str)
            except ValueError:
                logger.warning(
                    "ManualConfidenceSource: bad numeric in row %r "
                    "— skipping", row,
                )
                continue
            try:
                self.set(mid.strip(), tid.strip(), true_p=p, sources_count=n)
            except ValueError as exc:
                logger.warning(
                    "ManualConfidenceSource: rejected row %r: %s",
                    row, exc,
                )


__all__ = [
    "ConfidenceQuery",
    "ConfidenceResult",
    "ConfidenceSource",
    "ManualConfidenceSource",
]
