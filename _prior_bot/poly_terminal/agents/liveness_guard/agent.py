"""LivenessGuardAgent — derived `live_exit_ready` flag.

Watches the bus for the signals that prove the exit path is healthy
end-to-end:

  - EVT_MARKET_TICK  → ticks are flowing (WS or TickPoller)
  - EVT_AGENT_HEARTBEAT → boot loop is alive
  - EVT_DB_DEGRADED → DB is in a degraded state (writes may fail)

The agent doesn't OWN any of these subsystems — it just observes the
events they publish. State is in-memory only (resets on bot restart);
the startup grace window prevents a freshly-booted bot from rejecting
intents while it waits for the first tick to arrive.

Thresholds default to:
  - startup grace: 60s — enough time for WS connect + first tick
  - market tick age: 30s — empirically OK for active markets, will
    fail-fast if WS silent for half a minute
  - heartbeat age: 30s (boot loop publishes every 5s, plenty of margin)

Tunable via LivenessGuardConfig at construction.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_AGENT_HEARTBEAT,
    EVT_DB_DEGRADED,
    EVT_MARKET_TICK,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LivenessGuardConfig:
    startup_grace_s: float = 60.0
    """During the first N seconds after agent.start(), is_ready always
    returns True so a freshly-booted bot doesn't immediately reject
    intents while waiting for the first tick to arrive."""

    max_market_tick_age_s: float = 30.0
    """Reject if the most recent EVT_MARKET_TICK is older than this."""

    max_heartbeat_age_s: float = 30.0
    """Reject if the most recent EVT_AGENT_HEARTBEAT is older than this."""

    require_heartbeat: bool = True
    """Set False in tests that don't run the bot's heartbeat loop."""


class LivenessGuardAgent:
    """Subscribes to liveness signals; exposes is_ready()."""

    def __init__(
        self,
        bus: EventBus,
        cfg: LivenessGuardConfig | None = None,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._bus = bus
        self._cfg = cfg or LivenessGuardConfig()
        self._now = time_fn
        self._started_at: float = 0.0
        self._last_market_tick_ts: float = 0.0
        self._last_heartbeat_ts: float = 0.0
        self._db_degraded: bool = False
        self._started: bool = False
        self.stats: dict[str, int] = {
            "market_ticks_observed": 0,
            "heartbeats_observed": 0,
            "ready_checks": 0,
            "ready_rejects": 0,
            "db_degraded_flips": 0,
        }

    @property
    def cfg(self) -> LivenessGuardConfig:
        return self._cfg

    async def start(self) -> None:
        if self._started:
            return
        self._started_at = self._now()
        self._bus.subscribe(EVT_MARKET_TICK, self._on_market_tick)
        self._bus.subscribe(EVT_AGENT_HEARTBEAT, self._on_heartbeat)
        self._bus.subscribe(EVT_DB_DEGRADED, self._on_db_degraded)
        self._started = True

    async def _on_market_tick(self, _e: str, _payload: Any) -> None:
        self._last_market_tick_ts = self._now()
        self.stats["market_ticks_observed"] += 1

    async def _on_heartbeat(self, _e: str, _payload: Any) -> None:
        self._last_heartbeat_ts = self._now()
        self.stats["heartbeats_observed"] += 1

    async def _on_db_degraded(self, _e: str, payload: Any) -> None:
        # Payload may be {"degraded": True} or similar; treat any
        # publish as a degradation signal until something explicitly
        # un-flips it. Conservative — degraded DB is a sticky state
        # in this bot.
        flipped = bool(
            isinstance(payload, dict) and payload.get("degraded", True)
        )
        if flipped != self._db_degraded:
            self.stats["db_degraded_flips"] += 1
            logger.warning(
                "liveness_guard: db_degraded → %s",
                "DEGRADED" if flipped else "OK",
            )
        self._db_degraded = flipped

    def is_ready(self) -> tuple[bool, str | None]:
        """Returns (ready, reason_if_not). Reason is a stable string
        suitable for use as a Reject.detail."""
        self.stats["ready_checks"] += 1
        now = self._now()
        if not self._started:
            return False, "guard_not_started"
        # Startup grace — a freshly-booted bot hasn't seen a tick yet
        # but should still be allowed to enter trades. Without this
        # window, the very first intent of a session always rejects.
        elapsed_since_start = now - self._started_at
        if elapsed_since_start < self._cfg.startup_grace_s:
            return True, None
        if self._db_degraded:
            self.stats["ready_rejects"] += 1
            return False, "db_degraded"
        # Tick freshness — has anyone seen a market tick recently?
        if self._last_market_tick_ts == 0.0:
            self.stats["ready_rejects"] += 1
            return False, "no_market_tick_observed"
        tick_age = now - self._last_market_tick_ts
        if tick_age > self._cfg.max_market_tick_age_s:
            self.stats["ready_rejects"] += 1
            return False, f"market_tick_stale_{tick_age:.0f}s"
        # System heartbeat — the boot loop should be publishing every
        # ~5s. Stale heartbeat means the bot's main loop is wedged.
        if self._cfg.require_heartbeat:
            if self._last_heartbeat_ts == 0.0:
                self.stats["ready_rejects"] += 1
                return False, "no_heartbeat_observed"
            hb_age = now - self._last_heartbeat_ts
            if hb_age > self._cfg.max_heartbeat_age_s:
                self.stats["ready_rejects"] += 1
                return False, f"heartbeat_stale_{hb_age:.0f}s"
        return True, None

    def snapshot(self) -> dict[str, Any]:
        """For monitor / debug. Not used in hot path."""
        now = self._now()
        return {
            "started": self._started,
            "elapsed_since_start_s": (
                now - self._started_at if self._started else 0.0
            ),
            "last_market_tick_age_s": (
                now - self._last_market_tick_ts
                if self._last_market_tick_ts
                else None
            ),
            "last_heartbeat_age_s": (
                now - self._last_heartbeat_ts
                if self._last_heartbeat_ts
                else None
            ),
            "db_degraded": self._db_degraded,
            "stats": dict(self.stats),
            "cfg": {
                "startup_grace_s": self._cfg.startup_grace_s,
                "max_market_tick_age_s": self._cfg.max_market_tick_age_s,
                "max_heartbeat_age_s": self._cfg.max_heartbeat_age_s,
                "require_heartbeat": self._cfg.require_heartbeat,
            },
        }
