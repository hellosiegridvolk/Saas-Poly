"""LivenessGate — refuses new BUY entries when the live exit path is
degraded (deep-research-23 item #3).

Consults the LivenessGuardAgent for an aggregated `live_exit_ready`
flag covering market-tick freshness, system heartbeat, and DB health.

PAPER / READ_ONLY skip the gate cleanly — there's no live exit path
to protect. LIVE / LIVE_DRY / CLOSE_ONLY enforce.
"""

from __future__ import annotations

from typing import Callable, Protocol

from poly_terminal.shared.enums import BotMode
from poly_terminal.shared.typed_reject import Reject


class _Guard(Protocol):
    def is_ready(self) -> tuple[bool, str | None]:
        ...


class LivenessGate:
    def __init__(
        self,
        guard: _Guard,
        mode_getter: Callable[[], BotMode],
    ) -> None:
        self._guard = guard
        self._mode = mode_getter

    async def __call__(self, _intent: object) -> Reject | None:
        mode = self._mode()
        # PAPER + READ_ONLY don't expose us to live exit-path rot — no
        # need to gate. CLOSE_ONLY participates: even if we're not
        # opening fresh BUYs, the same liveness signals matter for
        # SELL evaluation, so the gate still runs (the SELL pipeline
        # bypasses it explicitly when wired only on the BUY pipeline).
        if mode in (BotMode.PAPER, BotMode.READ_ONLY):
            return None
        ready, reason = self._guard.is_ready()
        if ready:
            return None
        return Reject(
            code="live_exit_path_degraded",
            detail=reason or "unknown",
        )
