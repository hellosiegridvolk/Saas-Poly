"""Context Agent — per-market entry-window evaluator.

Emits EVT_CONTEXT_OK or EVT_CONTEXT_BLOCK with a typed `reason` so the
Strategy Agent can gate entries on a per-market basis without re-deriving
the criteria. Decisions are cached so repeated BLOCKs with the same
reason don't spam the bus; transitions OK→BLOCK and BLOCK→OK always emit.

Block reasons (stable strings):
  orderbook_disabled
  missing_end_date
  invalid_end_date
  time_left_below_floor
  oracle_window_proximity
  spread_above_ceiling
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Literal

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_CONTEXT_BLOCK,
    EVT_CONTEXT_OK,
)

logger = logging.getLogger(__name__)

NowReader = Callable[[], float]
Decision = Literal["OK", "BLOCK"]


@dataclass(frozen=True)
class ContextConfig:
    min_time_left_s: int = 60
    pre_oracle_s: int = 300
    max_spread_cents: Decimal = Decimal("5")


@dataclass(frozen=True)
class MarketContext:
    market_id: str
    end_date_iso: str | None
    spread_cents: Decimal | None
    enable_orderbook: bool


class ContextAgent:
    def __init__(
        self,
        bus: EventBus,
        cfg: ContextConfig | None = None,
        now_reader: NowReader | None = None,
    ) -> None:
        self._bus = bus
        self._cfg = cfg or ContextConfig()
        self._now = now_reader or (lambda: datetime.now(timezone.utc).timestamp())
        self._last_decision: dict[str, Decision] = {}
        self._last_block_reason: dict[str, str] = {}

    def last_decision(self, market_id: str) -> Decision | None:
        return self._last_decision.get(market_id)

    def last_block_reason(self, market_id: str) -> str | None:
        return self._last_block_reason.get(market_id)

    async def evaluate(self, ctx: MarketContext) -> None:
        reason = self._compute_block_reason(ctx)
        if reason is None:
            await self._emit_ok(ctx.market_id)
        else:
            await self._emit_block(ctx.market_id, reason)

    def _compute_block_reason(self, ctx: MarketContext) -> str | None:
        if not ctx.enable_orderbook:
            return "orderbook_disabled"
        if not ctx.end_date_iso:
            return "missing_end_date"
        try:
            end_dt = datetime.fromisoformat(
                str(ctx.end_date_iso).replace("Z", "+00:00")
            )
        except ValueError:
            return "invalid_end_date"
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        seconds_left = end_dt.timestamp() - self._now()
        if seconds_left < self._cfg.min_time_left_s:
            return "time_left_below_floor"
        if 0 <= seconds_left < self._cfg.pre_oracle_s:
            return "oracle_window_proximity"
        if ctx.spread_cents is None:
            return None  # spread is best-effort; absent = pass
        if ctx.spread_cents > self._cfg.max_spread_cents:
            return "spread_above_ceiling"
        return None

    async def _emit_ok(self, market_id: str) -> None:
        prev = self._last_decision.get(market_id)
        self._last_decision[market_id] = "OK"
        self._last_block_reason.pop(market_id, None)
        if prev == "OK":
            return
        await self._bus.publish(
            EVT_CONTEXT_OK,
            {"market_id": market_id, "ts": int(self._now())},
        )

    async def _emit_block(self, market_id: str, reason: str) -> None:
        prev = self._last_decision.get(market_id)
        prev_reason = self._last_block_reason.get(market_id)
        self._last_decision[market_id] = "BLOCK"
        self._last_block_reason[market_id] = reason
        if prev == "BLOCK" and prev_reason == reason:
            return
        await self._bus.publish(
            EVT_CONTEXT_BLOCK,
            {
                "market_id": market_id,
                "reason": reason,
                "ts": int(self._now()),
            },
        )
