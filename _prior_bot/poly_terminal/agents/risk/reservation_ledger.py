"""In-memory reservation ledger for in-flight BUY intents.

Background — Bug #2 (cap race), 2026-05-05 canary:
    The OpenPositionsGate reads `positions_repo.open_count()` to enforce
    `MAX_OPEN_POSITIONS`. When two BUY intents arrive in the same bus tick
    they call the reader concurrently, both observe `count = 0`, and both
    pass the gate. By the time either order's position row hits the
    `positions` table, the cap has already been over-committed. The
    canary (cap = 1) saw 4 BUYs fly through.

    The ledger closes the window between gate-pass and DB-write:

        gate-pass ─[reserve]─▶ in_flight ─[fill | TTL]─▶ released
                              ▲                          │
                              └─── readable by gate ─────┘

    The OpenPositionsGate uses the ledger as `count + in_flight >= max`
    under a lock so the read/check/reserve sequence is atomic. The
    RiskAgent releases on terminal order events and on late-gate
    rejections; the TTL is the last-resort safety net for paths that
    don't emit terminal events (e.g. execution-side early aborts).

The ledger is intentionally tiny:
    - Single in-memory dict keyed by intent_id
    - No persistence — restarting the bot drops all reservations and the
      DB-derived `open_count` becomes the source of truth again
    - No coupling to specific event types — RiskAgent owns subscriptions
"""

from __future__ import annotations

import time
from typing import Callable


class OpenPositionsReservationLedger:
    """Tracks risk-approved BUY intents until their orders reach a terminal
    state or expire by TTL. The TTL is a leak-prevention mechanism — under
    normal operation reservations are released promptly via terminal-event
    subscriptions in RiskAgent.

    Thread-safety: this object is not thread-safe by itself. It assumes
    callers serialize access via `asyncio` (single event loop), which is
    how the rest of the bot operates.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = 30.0,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
        self._reservations: dict[str, float] = {}
        self._ttl = float(ttl_seconds)
        self._now = time_fn

    def reserve(self, intent_id: str) -> None:
        """Mark `intent_id` as in-flight. Idempotent — re-reserving refreshes
        the timestamp (which is the desired behaviour if a partial-fill
        flow re-emits)."""
        if not intent_id:
            return
        self._reservations[intent_id] = self._now()

    def release(self, intent_id: str) -> None:
        """Remove `intent_id` from the ledger. No-op if absent."""
        if not intent_id:
            return
        self._reservations.pop(intent_id, None)

    def count(self) -> int:
        """Return the count of non-expired reservations.

        Sweeps stale entries (older than `ttl_seconds`) on each call so the
        gate never blocks indefinitely on a leaked reservation.
        """
        cutoff = self._now() - self._ttl
        stale = [k for k, ts in self._reservations.items() if ts < cutoff]
        for k in stale:
            self._reservations.pop(k, None)
        return len(self._reservations)

    def __len__(self) -> int:
        return self.count()

    def is_reserved(self, intent_id: str) -> bool:
        """True iff `intent_id` is currently in the ledger and not expired."""
        if not intent_id:
            return False
        ts = self._reservations.get(intent_id)
        if ts is None:
            return False
        if ts < self._now() - self._ttl:
            self._reservations.pop(intent_id, None)
            return False
        return True
