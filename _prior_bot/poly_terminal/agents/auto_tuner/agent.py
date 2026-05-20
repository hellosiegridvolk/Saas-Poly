"""AutoTunerAgent — adapt ProfitTaker thresholds to rolling PnL.

Every `interval_s` (default 15 min) the agent reads cumulative
realized PnL over `window_s` (default 1 hour) and adjusts
ProfitTakerAgent's profit/loss thresholds:

  - PnL <= loss_signal_usd (default -$1):
      tighten loss-cut by `step_pct` (default 15%) — exit losers
      faster; lower profit threshold by `step_pct` — lock wins
      sooner. Floors apply: `min_loss_threshold` and
      `min_profit_threshold`.
  - PnL >= profit_signal_usd (default +$3):
      loosen back to `default_*_threshold` (don't cap winners
      when the bot is performing well).
  - Otherwise: hold.

Bounded oscillation guard: once the agent tightens to the floor,
it can't tighten further; once it returns to default, it can't
loosen further. Cooldown of `interval_s` between adjustments.

Forward-compat: emits `EVT_AUTO_TUNED` with the before/after
thresholds + reason so dashboards can show the tuning history.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from poly_terminal.bus.event_bus import EventBus

logger = logging.getLogger(__name__)


class _ProfitTakerLike(Protocol):
    @property
    def cfg(self) -> object: ...
    def set_thresholds(
        self, profit_threshold_per_dollar: Decimal,
        loss_threshold_per_dollar: Decimal,
    ) -> None: ...


class _PositionsRepoLike(Protocol):
    async def realized_pnl_since(self, since_ts: int) -> tuple[int, float]: ...


@dataclass(frozen=True)
class AutoTunerConfig:
    interval_s: float = 900.0          # 15 min between sweeps
    window_s: int = 3600               # 1h rolling PnL window
    loss_signal_usd: float = -1.0      # tighten when PnL <= this
    profit_signal_usd: float = 3.0     # loosen when PnL >= this
    step_pct: Decimal = Decimal("0.15")  # tighten / loosen by this fraction
    default_profit_threshold: Decimal = Decimal("0.10")
    default_loss_threshold: Decimal = Decimal("0.10")
    min_profit_threshold: Decimal = Decimal("0.05")  # never below 5¢/$1
    min_loss_threshold: Decimal = Decimal("0.05")    # never above (tighter than) -5%


@dataclass
class AutoTunerStats:
    sweeps: int = 0
    tightenings: int = 0
    loosenings: int = 0
    last_window_pnl: float = 0.0
    last_window_closes: int = 0
    last_action: str = ""
    last_action_ts: int = 0


class AutoTunerAgent:
    def __init__(
        self,
        bus: EventBus,
        profit_taker: _ProfitTakerLike,
        positions_repo: _PositionsRepoLike,
        cfg: AutoTunerConfig | None = None,
    ) -> None:
        self._bus = bus
        self._pt = profit_taker
        self._positions = positions_repo
        self._cfg = cfg or AutoTunerConfig()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.stats = AutoTunerStats()

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
        # Don't tune on the very first interval — give the bot time to
        # accumulate at least one window of PnL.
        try:
            await asyncio.wait_for(
                self._stop.wait(), timeout=self._cfg.interval_s
            )
        except asyncio.TimeoutError:
            pass
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("auto_tuner: sweep failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._cfg.interval_s
                )
            except asyncio.TimeoutError:
                pass

    async def run_once(self) -> AutoTunerStats:
        """Public so scripts/tests can drive it manually."""
        now_ts = int(time.time())
        since_ts = now_ts - self._cfg.window_s
        n_closes, pnl = await self._positions.realized_pnl_since(since_ts)
        self.stats.sweeps += 1
        self.stats.last_window_pnl = pnl
        self.stats.last_window_closes = n_closes

        cur_cfg = self._pt.cfg
        cur_profit = Decimal(
            str(getattr(cur_cfg, "profit_threshold_per_dollar", "0.10"))
        )
        cur_loss = Decimal(
            str(getattr(cur_cfg, "loss_threshold_per_dollar", "0.10"))
        )

        action = "hold"
        new_profit = cur_profit
        new_loss = cur_loss

        if pnl <= self._cfg.loss_signal_usd:
            # Tighten: lower profit threshold (lock wins sooner) AND
            # lower loss threshold (cut losses sooner).
            tightened_profit = cur_profit * (Decimal("1") - self._cfg.step_pct)
            tightened_loss = cur_loss * (Decimal("1") - self._cfg.step_pct)
            new_profit = max(tightened_profit, self._cfg.min_profit_threshold)
            new_loss = max(tightened_loss, self._cfg.min_loss_threshold)
            if new_profit != cur_profit or new_loss != cur_loss:
                action = "tighten"
                self.stats.tightenings += 1
        elif pnl >= self._cfg.profit_signal_usd:
            # Loosen back to defaults to capture larger trends.
            if (
                cur_profit < self._cfg.default_profit_threshold
                or cur_loss < self._cfg.default_loss_threshold
            ):
                new_profit = self._cfg.default_profit_threshold
                new_loss = self._cfg.default_loss_threshold
                action = "loosen"
                self.stats.loosenings += 1

        if action != "hold":
            self._pt.set_thresholds(new_profit, new_loss)
            self.stats.last_action = action
            self.stats.last_action_ts = now_ts
            logger.warning(
                "auto_tuner: %s — pnl=$%.2f over %ds (%d closes); "
                "profit %.4f → %.4f, loss %.4f → %.4f",
                action.upper(), pnl, self._cfg.window_s, n_closes,
                cur_profit, new_profit, cur_loss, new_loss,
            )
        else:
            logger.info(
                "auto_tuner: hold — pnl=$%.2f over %ds (%d closes); "
                "profit=%.4f loss=%.4f",
                pnl, self._cfg.window_s, n_closes, cur_profit, cur_loss,
            )
        return self.stats
