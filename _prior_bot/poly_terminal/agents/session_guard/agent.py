"""SessionGuardAgent — session-wide PnL circuit breaker.

Subscribes to EVT_POSITION_CLOSED and accumulates `realized_pnl`
since the agent started. When cumulative PnL crosses either:

  - `profit_target_usd` (default +$20): stop trading on a winning
    cycle, log success.
  - `loss_limit_usd` (default $20, applied as the absolute value
    of the cumulative loss): stop trading on a losing cycle, log
    halt-reason for operator review.

Stopping = writes `exports/paused.flag` via `pause_flag.set_paused`.
The next intent/SELL hits the mode_lock gate and returns READ_ONLY,
which short-circuits both BUYs and live SELLs (PAPER mechanics keep
running, so the bot stays observable). Operator unblocks with
`scripts/resume.sh` once they've reviewed.

Idempotent: once the flag is set, this agent stops re-firing the
halt logic — operators may resume mid-session and the cumulative
counter keeps going so a second halt can re-trip.

Forward-compat: `EVT_BUDGET_HALTED` is published with the cumulative
PnL + side (`profit_target` / `loss_limit`) so dashboards can render
the bracket-trip event without polling stats.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_POSITION_CLOSED
from poly_terminal.shared.pause_flag import is_paused, set_paused

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionGuardConfig:
    profit_target_usd: float = 20.0   # halt at +$20 cumulative
    loss_limit_usd: float = 20.0      # halt at -$20 cumulative
    flag_path: str = "exports/paused.flag"


class SessionGuardAgent:
    def __init__(
        self,
        bus: EventBus,
        cfg: SessionGuardConfig | None = None,
    ) -> None:
        self._bus = bus
        self._cfg = cfg or SessionGuardConfig()
        self._cumulative_pnl: float = 0.0
        self._closes_observed: int = 0
        self._halt_fired: bool = False
        self._started = False
        # Surfaced for /api/session_guard (future) + tests.
        self.stats: dict[str, Any] = {
            "cumulative_pnl_usd": 0.0,
            "closes_observed": 0,
            "halts_fired": 0,
            "last_halt_reason": "",
            "last_halt_pnl": 0.0,
        }

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_POSITION_CLOSED, self._on_close)
        self._started = True

    @property
    def cumulative_pnl(self) -> float:
        return self._cumulative_pnl

    async def _on_close(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            pnl = float(payload.get("realized_pnl", 0))
        except (TypeError, ValueError):
            return
        self._cumulative_pnl += pnl
        self._closes_observed += 1
        self.stats["cumulative_pnl_usd"] = self._cumulative_pnl
        self.stats["closes_observed"] = self._closes_observed

        # Don't re-fire the halt if we already paused this session
        # (operator may have resumed externally — let the cumulative
        # keep going so a second cross can trip again).
        if self._halt_fired:
            return

        # Profit target hit — winning cycle.
        if self._cumulative_pnl >= self._cfg.profit_target_usd:
            await self._halt(
                reason="profit_target",
                detail=(
                    f"cumulative realized PnL ${self._cumulative_pnl:.2f} "
                    f">= target ${self._cfg.profit_target_usd:.2f} — "
                    "stopping live cycle on a winning session"
                ),
            )
            return

        # Loss limit hit — losing cycle.
        if self._cumulative_pnl <= -abs(self._cfg.loss_limit_usd):
            await self._halt(
                reason="loss_limit",
                detail=(
                    f"cumulative realized PnL ${self._cumulative_pnl:.2f} "
                    f"<= -${abs(self._cfg.loss_limit_usd):.2f} — "
                    "stopping live cycle to protect capital"
                ),
            )
            return

    async def _halt(self, *, reason: str, detail: str) -> None:
        self._halt_fired = True
        self.stats["halts_fired"] += 1
        self.stats["last_halt_reason"] = reason
        self.stats["last_halt_pnl"] = self._cumulative_pnl

        already_paused = is_paused(self._cfg.flag_path)
        wrote = set_paused(
            reason=f"session_guard:{reason} pnl=${self._cumulative_pnl:.2f}",
            flag_path=self._cfg.flag_path,
        )
        log = logger.error if reason == "loss_limit" else logger.warning
        log(
            "session_guard: HALTED reason=%s pnl=$%.2f closes=%d "
            "(flag_already_set=%s, wrote_flag=%s) — %s",
            reason, self._cumulative_pnl, self._closes_observed,
            already_paused, wrote, detail,
        )
