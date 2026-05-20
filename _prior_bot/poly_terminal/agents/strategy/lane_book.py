"""LaneBook — per-lane risk partitioning over the pure RiskAllocator.

A lane = (strategy, named parameter set). `Lane.id` becomes a strategy
instance's `.name`, so it flows verbatim into paper_fills.strategy and
positions.lane_id. LaneBook is duck-typed to RiskAllocator.approve so
it drops into the existing `allocator=` injection point unchanged.
Per-trade sizing is set by threading Lane.per_trade_cap_usd into the
strategy's own config (see build_bakeoff_strategies); LaneBook enforces
per-lane bankroll / exposure / daily-loss isolation only.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from poly_terminal.agents.strategy.allocator import (
    AllocatorConfig,
    LedgerSnapshot,
    RiskAllocator,
)
from poly_terminal.agents.strategy.framework import (
    RejectReason,
    StrategyDecision,
    StrategySignal,
)
from poly_terminal.shared.enums import BotMode

logger = logging.getLogger(__name__)

_MARKET_SCOPES = frozenset({"all", "crypto_bars"})


@dataclass(frozen=True)
class Lane:
    id: str
    strategy: str
    bankroll_usd: float
    per_trade_cap_usd: float
    daily_loss_cap_usd: float
    market_scope: str
    params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


def load_lanes(path: str | Path) -> list[Lane]:
    """Parse + validate the structured lane grid. Raises ValueError."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise ValueError("lanes file must be a mapping with version: 1")
    rows = raw.get("lanes")
    if not isinstance(rows, list) or not rows:
        raise ValueError("lanes file must contain a non-empty 'lanes' list")
    lanes: list[Lane] = []
    seen: set[str] = set()
    for r in rows:
        lid = str(r["id"])
        if lid in seen:
            raise ValueError(f"duplicate lane id: {lid}")
        seen.add(lid)
        scope = str(r["market_scope"])
        if scope not in _MARKET_SCOPES:
            raise ValueError(
                f"lane {lid}: market_scope {scope!r} not in "
                f"{sorted(_MARKET_SCOPES)}"
            )
        lanes.append(
            Lane(
                id=lid,
                strategy=str(r["strategy"]),
                bankroll_usd=float(r["bankroll_usd"]),
                per_trade_cap_usd=float(r["per_trade_cap_usd"]),
                daily_loss_cap_usd=float(r["daily_loss_cap_usd"]),
                market_scope=scope,
                params=dict(r.get("params", {}) or {}),
                enabled=bool(r.get("enabled", True)),
            )
        )
    return lanes


class LaneBook:
    """Duck-typed RiskAllocator. Routes each signal to its lane, builds
    a lane-scoped LedgerSnapshot + AllocatorConfig, delegates to a
    per-call pure RiskAllocator. Lane id == signal.strategy_name (the
    overridden instance .name).

    live_position_cap_usd is set to the lane BANKROLL (a non-binding
    backstop): per-trade size is controlled by the strategy's own config
    (Lane.per_trade_cap_usd is threaded there in build_bakeoff_
    strategies). The binding per-lane controls here are bankroll
    (== max_total_exposure) and daily_loss_cap.
    """

    def __init__(
        self,
        lanes: list[Lane],
        *,
        lane_realized_getter: Callable[[str], float],
    ) -> None:
        self._by_id: dict[str, Lane] = {ln.id: ln for ln in lanes}
        self._lane_realized = lane_realized_getter

    def approve(
        self,
        signal: StrategySignal,
        *,
        mode: BotMode,
        ledger: LedgerSnapshot,
    ) -> StrategyDecision:
        lane = self._by_id.get(signal.strategy_name)
        if lane is None:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.STRATEGY_DISABLED,
                detail=f"no lane registered for {signal.strategy_name!r}",
            )
        if not lane.enabled:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.STRATEGY_DISABLED,
                detail=f"lane {lane.id!r} is paused (enabled=false)",
            )
        lane_positions = tuple(
            p for p in ledger.open_positions
            if p.strategy_name == lane.id
        )
        lane_ledger = LedgerSnapshot(
            open_positions=lane_positions,
            realized_today_usd=float(self._lane_realized(lane.id)),
            quarantined_tokens=ledger.quarantined_tokens,
        )
        lane_cfg = AllocatorConfig(
            bankroll_usd=lane.bankroll_usd,
            live_position_cap_usd=lane.bankroll_usd,
            open_position_limit=10_000,
            max_total_exposure_usd=lane.bankroll_usd,
            daily_loss_cap_usd=lane.daily_loss_cap_usd,
            one_strategy_at_a_time=False,
            live_allowed=frozenset(),
            wallet_probation_min_paper_fills=0,
        )
        return RiskAllocator(lane_cfg).approve(
            signal, mode=mode, ledger=lane_ledger
        )


_CACHE_REFRESH_INTERVAL_S = 15.0
_CACHE_DAY_WINDOW_S = 86_400


class LaneRealizedCache:
    """Background-refreshed {lane_id: realized_today_usd}. get() is
    synchronous and safe in the allocator path (whole dict reassigned
    atomically; no await between a get() read and its use). Mirrors
    LedgerSnapshotRefresher; does not modify it."""

    def __init__(
        self,
        *,
        positions_repo: Any,
        lane_ids: list[str],
        refresh_interval_s: float = _CACHE_REFRESH_INTERVAL_S,
        day_window_s: int = _CACHE_DAY_WINDOW_S,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._repo = positions_repo
        self._lane_ids = list(lane_ids)
        self._interval = float(refresh_interval_s)
        self._day_window_s = int(day_window_s)
        self._clock = clock
        self._cached: dict[str, float] = {}
        self._task: asyncio.Task | None = None
        self._stopping = False

    def get(self, lane_id: str) -> float:
        return float(self._cached.get(lane_id, 0.0))

    async def refresh_once(self) -> None:
        since = int(self._clock()) - self._day_window_s
        fresh: dict[str, float] = {}
        for lid in self._lane_ids:
            fresh[lid] = float(
                await self._repo.realized_pnl_since_for_lane(lid, since)
            )
        self._cached = fresh

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        try:
            await self.refresh_once()
        except Exception as exc:
            logger.warning("lane_realized_cache: initial refresh failed: %s", exc)
        self._task = asyncio.create_task(
            self._loop(), name="lane_realized_cache"
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while not self._stopping:
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("lane_realized_cache: refresh failed: %s", exc)
            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                raise
