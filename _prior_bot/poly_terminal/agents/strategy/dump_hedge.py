"""Dump-and-hedge — buy the un-dumped leg when one side crashes.

ADR 0003: classic YES+NO arb is structurally blocked. Dump-hedge is the
viable variant — wait for one leg to drop hard enough that buying the
opposite leg locks in a profit at resolution.

State:
  - per-token recent price history (bounded ring)
  - mapping token_id → (market_id, opposite_token_id)
  - last-fire ts per market for cooldown
"""

from __future__ import annotations

import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.agents.strategy.exit_config import for_strategy
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_BUY_INTENT,
    EVT_CONTEXT_BLOCK,
    EVT_CONTEXT_OK,
    EVT_MARKET_TICK,
    EVT_WATCHLIST_UPDATED,
)
from poly_terminal.shared.enums import IntentSide, IntentSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DumpHedgeConfig:
    dump_pct: Decimal = Decimal("0.15")
    lookback_s: int = 3
    target_edge_pct: Decimal = Decimal("0.05")  # combined < 1 - edge
    size_usd: Decimal = Decimal("10")
    cooldown_s: int = 60
    history_capacity: int = 256


@dataclass
class _TokenLink:
    market_id: str
    opposite_token_id: str
    end_date_iso: str


@dataclass
class _PricePoint:
    ts: int
    price: Decimal


class DumpHedgeStrategy(BaseStrategy):
    name = "dump_hedge"

    def __init__(
        self,
        bus: EventBus,
        cfg: DumpHedgeConfig | None = None,
        *,
        # 2026-05-10 Phase 32 P3 — RiskAllocator gate (BaseStrategy).
        allocator: Any | None = None,
        mode_getter: Any | None = None,
        ledger_snapshot_getter: Any | None = None,
    ) -> None:
        super().__init__(
            bus,
            allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        self._cfg = cfg or DumpHedgeConfig()
        self._link: dict[str, _TokenLink] = {}
        self._latest_price: dict[str, Decimal] = {}
        self._history: dict[str, deque[_PricePoint]] = {}
        self._market_blocked: set[str] = set()
        self._last_fire_ts: dict[str, int] = {}

    async def _subscribe(self) -> None:
        self._bus.subscribe(EVT_WATCHLIST_UPDATED, self._on_watchlist)
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_ctx_ok)
        self._bus.subscribe(EVT_CONTEXT_BLOCK, self._on_ctx_block)
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)

    async def _on_watchlist(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        for m in payload.get("markets", []):
            mid = str(m.get("market_id", ""))
            yes = str(m.get("token_yes", ""))
            no = str(m.get("token_no", ""))
            end_iso = str(m.get("end_date_iso", ""))
            if not (mid and yes and no):
                continue
            self._link[yes] = _TokenLink(market_id=mid, opposite_token_id=no, end_date_iso=end_iso)
            self._link[no] = _TokenLink(market_id=mid, opposite_token_id=yes, end_date_iso=end_iso)

    async def _on_ctx_ok(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self._market_blocked.discard(str(payload.get("market_id", "")))

    async def _on_ctx_block(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self._market_blocked.add(str(payload.get("market_id", "")))

    async def _on_tick(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        token = str(payload.get("token_id", ""))
        link = self._link.get(token)
        if link is None:
            return
        try:
            ts = int(payload.get("ts", 0))
            price = Decimal(str(payload["price"]))
        except (KeyError, TypeError, ValueError):
            return
        self._latest_price[token] = price
        hist = self._history.setdefault(
            token, deque(maxlen=self._cfg.history_capacity)
        )
        hist.append(_PricePoint(ts=ts, price=price))

        if link.market_id in self._market_blocked:
            return

        last = self._last_fire_ts.get(link.market_id)
        if last is not None and (ts - last) < self._cfg.cooldown_s:
            return

        # Detect a dump on this leg over the lookback window.
        baseline = self._lookback_baseline(token, ts)
        if baseline is None:
            return
        if baseline <= 0:
            return
        drop = (baseline - price) / baseline
        if drop < self._cfg.dump_pct:
            return

        # Confirm combined cost < 1 - edge.
        opp_price = self._latest_price.get(link.opposite_token_id)
        if opp_price is None:
            return
        combined = price + opp_price
        if combined >= (Decimal("1") - self._cfg.target_edge_pct):
            return

        # Hedge by buying the opposite leg.
        await self._emit(
            link, dumped_token=token, hedge_token=link.opposite_token_id,
            hedge_price=opp_price, ts=ts,
        )

    def _lookback_baseline(self, token: str, now_ts: int) -> Decimal | None:
        hist = self._history.get(token)
        if not hist or len(hist) < 2:
            return None
        cutoff = now_ts - self._cfg.lookback_s
        for point in hist:
            if point.ts >= cutoff and point.ts < now_ts:
                return point.price
        return None

    async def _emit(
        self,
        link: _TokenLink,
        *,
        dumped_token: str,
        hedge_token: str,
        hedge_price: Decimal,
        ts: int,
    ) -> None:
        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=link.market_id,
            token_id=hedge_token,
            side=IntentSide.BUY,
            size_usd=self._cfg.size_usd,
            limit_price=hedge_price,
            source=IntentSource.DUMP_HEDGE,
            created_at=float(ts),
            end_date_iso=link.end_date_iso,
            exit_config=for_strategy(self.name),
        )

        # 2026-05-10 Phase 32 P3 — RiskAllocator gate.
        if not self._allocator_approves_intent(
            market_id=link.market_id,
            token_id=hedge_token,
            size_usd=float(self._cfg.size_usd),
            marketable_price=float(hedge_price),
        ):
            return

        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
        self._last_fire_ts[link.market_id] = ts
