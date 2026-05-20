"""Base class for strategy agents.

Each strategy subscribes to its trigger event(s), maintains its own state,
and emits `EVT_BUY_INTENT` with a populated `BuyIntent` (including the
strategy's `ExitConfig`). The Risk Agent decides whether to approve.

2026-05-10 PHASE 32 P3 — every strategy now has built-in optional
support for the framework's `RiskAllocator` gate. Subclasses pass the
allocator + mode_getter + ledger_snapshot_getter kwargs through their
__init__ to BaseStrategy, then call `self._allocator_approves_intent(...)`
right before publishing EVT_BUY_INTENT. None values keep legacy
behavior (tests + paper-only).
"""

from __future__ import annotations

import logging
from abc import ABC
from typing import Any, Callable

from poly_terminal.bus.event_bus import EventBus

logger = logging.getLogger(__name__)


class BaseStrategy(ABC):
    """Common boilerplate for all strategies."""

    name: str = "base"

    def __init__(
        self,
        bus: EventBus,
        *,
        # Phase 32 P3 — optional RiskAllocator gate.
        allocator: Any | None = None,
        mode_getter: Callable[[], Any] | None = None,
        ledger_snapshot_getter: Callable[[], Any] | None = None,
    ) -> None:
        self._bus = bus
        self._started = False
        self.intents_emitted = 0
        self._allocator = allocator
        self._mode_getter = mode_getter
        self._ledger_snapshot_getter = ledger_snapshot_getter
        self.intents_rejected_allocator: int = 0

    async def start(self) -> None:
        if self._started:
            return
        await self._subscribe()
        self._started = True

    async def _subscribe(self) -> None:
        """Override in subclasses to bind handlers to bus events."""
        raise NotImplementedError

    # ── Phase 32 P3 — shared RiskAllocator gate ────────────────────
    def _allocator_approves_intent(
        self,
        *,
        market_id: str,
        token_id: str,
        size_usd: float,
        marketable_price: float,
        extra: dict | None = None,
    ) -> bool:
        """Run the RiskAllocator gate before publishing an intent.

        Returns True to proceed (publish), False to drop. Default
        behavior when `_allocator is None` is to proceed (legacy
        path; tests + paper-only deploys keep the helper a no-op).

        `extra` is the StrategySignal.extra dict — copy strategies pass
        wallet metadata so the probation gate fires correctly; non-copy
        strategies can leave it empty.
        """
        if self._allocator is None:
            return True
        # Lazy imports — keep BaseStrategy free of framework deps for
        # tests that don't exercise the allocator path.
        from poly_terminal.agents.strategy.allocator import LedgerSnapshot
        from poly_terminal.agents.strategy.framework import StrategySignal
        from poly_terminal.shared.enums import BotMode

        try:
            signal = StrategySignal(
                strategy_name=self.name,
                market_id=market_id,
                token_id=token_id,
                side="YES",
                confidence=0.6,
                edge_bps=100,
                max_loss_usd=size_usd,
                target_exit=min(0.99, marketable_price + 0.05),
                stop_exit=max(0.01, marketable_price - 0.10),
                max_hold_s=24 * 3600,
                extra=extra or {},
            )
            mode = (
                self._mode_getter() if self._mode_getter is not None
                else BotMode.PAPER
            )
            ledger = (
                self._ledger_snapshot_getter()
                if self._ledger_snapshot_getter is not None
                else LedgerSnapshot()
            )
            decision = self._allocator.approve(
                signal, mode=mode, ledger=ledger,
            )
        except Exception:
            logger.exception(
                "%s: allocator approve raised; rejecting intent for "
                "safety (token=%s)", self.name, token_id,
            )
            self.intents_rejected_allocator += 1
            return False
        if not decision.approved:
            logger.info(
                "%s: allocator REJECTED intent — reason=%s detail=%s "
                "(token=%s)",
                self.name,
                decision.reason.value if decision.reason else "unknown",
                decision.detail, token_id,
            )
            self.intents_rejected_allocator += 1
            return False
        return True
