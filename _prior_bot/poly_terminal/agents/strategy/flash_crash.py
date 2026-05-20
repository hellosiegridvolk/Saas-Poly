"""Flash-crash strategy — composes ConfirmedPeakDetector with bus wiring.

Subscribes to EVT_MARKET_TICK; per-token detector tracks recent bars and
emits EVT_BUY_INTENT when peak persistence + drop threshold + size surge
all agree. Context-blocked markets are skipped silently.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.agents.strategy.exit_config import for_strategy
from poly_terminal.agents.strategy.peak_detector import (
    Bar,
    ConfirmedPeakDetector,
    PeakConfig,
)
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_BUY_INTENT,
    EVT_CONTEXT_BLOCK,
    EVT_CONTEXT_OK,
    EVT_MARKET_TICK,
)
from poly_terminal.shared.enums import IntentSide, IntentSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FlashCrashConfig:
    peak_cfg: PeakConfig = field(default_factory=PeakConfig)
    size_usd: Decimal = Decimal("10")
    cooldown_s: int = 60   # don't re-fire on the same token for N seconds


class FlashCrashStrategy(BaseStrategy):
    name = "flash_crash"

    def __init__(
        self,
        bus: EventBus,
        cfg: FlashCrashConfig | None = None,
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
        self._cfg = cfg or FlashCrashConfig()
        self._detectors: dict[str, ConfirmedPeakDetector] = {}
        self._market_blocked: set[str] = set()
        self._token_market: dict[str, str] = {}
        self._token_end_date: dict[str, str] = {}
        self._last_fire_ts: dict[str, int] = {}

    async def _subscribe(self) -> None:
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_ctx_ok)
        self._bus.subscribe(EVT_CONTEXT_BLOCK, self._on_ctx_block)

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
        market = str(payload.get("market_id", ""))
        if not token or not market:
            return
        try:
            price = Decimal(str(payload["price"]))
            size = Decimal(str(payload.get("size", 0)))
            ts = int(payload.get("ts", 0))
        except (KeyError, TypeError, ValueError):
            return
        if payload.get("end_date_iso"):
            self._token_end_date[token] = str(payload["end_date_iso"])
        self._token_market[token] = market

        det = self._detectors.get(token)
        if det is None:
            det = ConfirmedPeakDetector(self._cfg.peak_cfg)
            self._detectors[token] = det
        det.add_bar(Bar(ts=float(ts), price=price, size=size))

        if market in self._market_blocked:
            return
        last = self._last_fire_ts.get(token)
        if last is not None and (ts - last) < self._cfg.cooldown_s:
            return

        if det.should_fire(current_price=price):
            await self._emit(token, market, price, ts)

    async def _emit(
        self, token: str, market: str, price: Decimal, ts: int
    ) -> None:
        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=market,
            token_id=token,
            side=IntentSide.BUY,
            size_usd=self._cfg.size_usd,
            limit_price=price,
            source=IntentSource.FLASH_CRASH,
            created_at=float(ts),
            end_date_iso=self._token_end_date.get(token),
            exit_config=for_strategy(self.name),
        )

        # 2026-05-10 Phase 32 P3 — RiskAllocator gate.
        if not self._allocator_approves_intent(
            market_id=market,
            token_id=token,
            size_usd=float(self._cfg.size_usd),
            marketable_price=float(price),
        ):
            return

        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
        self._last_fire_ts[token] = ts
