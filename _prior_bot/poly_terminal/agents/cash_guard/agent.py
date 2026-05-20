"""CashGuardAgent — pause new BUYs when portfolio + cash crosses a
threshold. Cooldown until the top of the next hour.

Watches realized + unrealized PnL across the bot's tracked positions
plus a "starting cash" baseline (set once at boot). When the
sum-since-baseline exceeds `profit_threshold_usd`, the agent flips
the BuyCooldownGate's `paused_until_ts` to the top of the next
hour. SELL exits / ProfitTaker / SessionGuard / Redeemer all keep
running — only new BUY entries get rejected at the gate.

Resumes automatically when the cooldown expires (gate compares to
current time on every check).

Polling: every `interval_s` (default 60s). Repeated triggers within
the same cooldown window are no-ops (cooldown stays at the same
top-of-hour timestamp).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Protocol

from poly_terminal.bus.event_bus import EventBus

logger = logging.getLogger(__name__)


class _PositionsRepoLike(Protocol):
    async def realized_pnl_since(
        self, since_ts: int
    ) -> tuple[int, float]: ...


@dataclass(frozen=True)
class CashGuardConfig:
    interval_s: float = 60.0
    profit_threshold_usd: float = 10.0  # trigger at +$10 cumulative
    cooldown_grace_s: int = 0           # extra seconds past top-of-hour


@dataclass
class CashGuardStats:
    sweeps: int = 0
    triggers: int = 0
    last_window_pnl: float = 0.0
    last_trigger_ts: int = 0
    paused_until_ts: int = 0


def _next_hour_ts(now_ts: int, grace_s: int = 0) -> int:
    """Return the unix-seconds timestamp of the next top-of-hour
    (UTC), plus optional grace. e.g. now=15:44 → 16:00."""
    bucket = (now_ts // 3600 + 1) * 3600
    return bucket + grace_s


class CashGuardAgent:
    def __init__(
        self,
        bus: EventBus,
        positions_repo: _PositionsRepoLike,
        cfg: CashGuardConfig | None = None,
    ) -> None:
        self._bus = bus
        self._positions = positions_repo
        self._cfg = cfg or CashGuardConfig()
        # baseline_ts pinned at boot — PnL is measured since the
        # bot started, so a fresh restart resets the "earned" window.
        self._baseline_ts: int = int(time.time())
        self._paused_until_ts: int = 0
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.stats = CashGuardStats()

    @property
    def paused_until_ts(self) -> int:
        return self._paused_until_ts

    def get_paused_until(self) -> int:
        """Callable shape for BuyCooldownGate."""
        return self._paused_until_ts

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("cash_guard: sweep failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._cfg.interval_s
                )
            except asyncio.TimeoutError:
                pass

    async def run_once(self) -> CashGuardStats:
        """Public so tests + scripts can drive it manually."""
        now_ts = int(time.time())
        _, pnl = await self._positions.realized_pnl_since(self._baseline_ts)
        self.stats.sweeps += 1
        self.stats.last_window_pnl = pnl
        # Already paused? Just re-record stats and return.
        if self._paused_until_ts > now_ts:
            return self.stats
        # Threshold breach → set cooldown to top of next hour.
        if pnl >= self._cfg.profit_threshold_usd:
            until_ts = _next_hour_ts(now_ts, self._cfg.cooldown_grace_s)
            self._paused_until_ts = until_ts
            self.stats.triggers += 1
            self.stats.last_trigger_ts = now_ts
            self.stats.paused_until_ts = until_ts
            logger.warning(
                "cash_guard: THRESHOLD HIT — cumulative PnL $%.2f >= "
                "$%.2f. Pausing new BUYs until %d (%.1f min from now). "
                "SELLs / exits / redeemer continue normally.",
                pnl, self._cfg.profit_threshold_usd, until_ts,
                (until_ts - now_ts) / 60.0,
            )
        else:
            logger.info(
                "cash_guard: hold — pnl $%.2f / threshold $%.2f",
                pnl, self._cfg.profit_threshold_usd,
            )
        return self.stats
