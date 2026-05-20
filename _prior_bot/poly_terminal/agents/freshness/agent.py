"""Phase 30(b) — exit-path freshness tracker.

The deep-research-report (26)/(27) re-investigations of v12 surfaced
a class of failure that container/process health checks cannot
detect: an open live position whose tick stream is silently dead.
The reports recommend an APPLICATION-LEVEL flag — `live_canary_ready`
— that flips false the moment any held position becomes tick-blind.

This module implements that. It subscribes to:
  - EVT_POSITION_OPENED  → register a position as held
  - EVT_POSITION_CLOSED  → de-register
  - EVT_MARKET_TICK      → update last_tick_ts per token
  - EVT_EXIT_DECISION_RECORDED → update last_eval_ts per position
                                 (publish-after-eval is the right
                                  proxy; ExitDecisionEngine emits
                                  one such event per evaluation)

Then exposes a snapshot:
    {
        "positions": {
            position_id: {
                "token_id": ...,
                "entry_ts": ...,
                "last_tick_age_ms": ...,
                "last_exit_eval_age_ms": ...,
            },
            ...
        },
        "live_canary_ready": bool,   # True iff every held position
                                     # has fresh tick + eval ages.
    }

`live_canary_ready` becomes False when ANY held position has either
last_tick_age_ms > tick_stale_ms OR last_exit_eval_age_ms >
eval_stale_ms.

In-memory only — observability surface, not a transactional record.
The recorded exit_evals table is the durable artifact.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_MARKET_TICK,
    EVT_POSITION_CLOSED,
    EVT_POSITION_OPENED,
)

logger = logging.getLogger(__name__)


# Bus name for ExitDecisionEngine evaluation records. We import lazily
# below to avoid coupling this module to ExitAgent.
EVT_EXIT_EVAL = "exit.eval.recorded"


@dataclass
class _PositionFreshness:
    position_id: int
    token_id: str
    entry_ts: float
    last_tick_ts: float = 0.0
    last_eval_ts: float = 0.0


class FreshnessTracker:
    """Tracks per-position tick + eval freshness; publishes a
    `live_canary_ready` rollup."""

    def __init__(
        self,
        bus: EventBus,
        tick_stale_ms: int = 30_000,        # 30s tick-blind threshold
        eval_stale_ms: int = 60_000,        # 60s no-evaluation threshold
        now_fn: Any = None,
    ) -> None:
        self._bus = bus
        self._tick_stale_ms = tick_stale_ms
        self._eval_stale_ms = eval_stale_ms
        self._now = now_fn or (lambda: time.time())
        self._positions: dict[int, _PositionFreshness] = {}
        # token_id → last_tick_ts (shared across positions on same token)
        self._token_last_tick: dict[str, float] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_POSITION_OPENED, self._on_open)
        self._bus.subscribe(EVT_POSITION_CLOSED, self._on_close)
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)
        # Best-effort subscribe to the exit-eval event (string name —
        # if no publisher emits it, no harm). ExitDecisionEngine /
        # ProfitTaker can publish on this name to feed eval freshness.
        self._bus.subscribe(EVT_EXIT_EVAL, self._on_eval)
        self._started = True

    async def _on_open(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            pid = int(payload["position_id"])
            tok = str(payload.get("token_id", ""))
        except (KeyError, TypeError, ValueError):
            return
        try:
            entry_ts = float(payload.get("entry_ts", 0))
        except (TypeError, ValueError):
            entry_ts = 0.0
        now = self._now()
        self._positions[pid] = _PositionFreshness(
            position_id=pid,
            token_id=tok,
            entry_ts=entry_ts or now,
            last_tick_ts=self._token_last_tick.get(tok, 0.0),
            last_eval_ts=now,  # treat open as a freshness anchor
        )

    async def _on_close(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            pid = int(payload["position_id"])
        except (KeyError, TypeError, ValueError):
            return
        self._positions.pop(pid, None)

    async def _on_tick(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        tok = str(payload.get("token_id", ""))
        if not tok:
            return
        try:
            ts = float(payload.get("ts") or self._now())
        except (TypeError, ValueError):
            ts = self._now()
        self._token_last_tick[tok] = ts
        # Also fan out to any held position on this token.
        for pf in self._positions.values():
            if pf.token_id == tok and ts > pf.last_tick_ts:
                pf.last_tick_ts = ts

    async def _on_eval(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            pid = int(payload["position_id"])
        except (KeyError, TypeError, ValueError):
            return
        try:
            eval_ts = float(payload.get("eval_ts") or self._now())
        except (TypeError, ValueError):
            eval_ts = self._now()
        pf = self._positions.get(pid)
        if pf and eval_ts > pf.last_eval_ts:
            pf.last_eval_ts = eval_ts

    def snapshot(self) -> dict[str, Any]:
        """Returns the current freshness rollup. Safe to call from
        any thread / route."""
        now = self._now()
        positions: dict[int, dict[str, Any]] = {}
        live_ready = True
        for pid, pf in self._positions.items():
            tick_age = (
                int((now - pf.last_tick_ts) * 1000)
                if pf.last_tick_ts > 0 else None
            )
            eval_age = (
                int((now - pf.last_eval_ts) * 1000)
                if pf.last_eval_ts > 0 else None
            )
            positions[pid] = {
                "token_id": pf.token_id,
                "entry_ts": pf.entry_ts,
                "last_tick_age_ms": tick_age,
                "last_exit_eval_age_ms": eval_age,
            }
            tick_blind = (
                tick_age is None or tick_age > self._tick_stale_ms
            )
            eval_stale = (
                eval_age is None or eval_age > self._eval_stale_ms
            )
            if tick_blind or eval_stale:
                live_ready = False
        # No positions held → not "ready" in the trading-canary sense
        # is operator-debatable. We report True (no canary risk) when
        # there's nothing to babysit.
        return {
            "positions": positions,
            "live_canary_ready": live_ready,
            "tick_stale_ms_threshold": self._tick_stale_ms,
            "eval_stale_ms_threshold": self._eval_stale_ms,
        }
