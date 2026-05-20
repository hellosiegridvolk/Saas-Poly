"""Orderbook Intelligence Agent — bus orchestrator.

Subscribes to EVT_BOOK_SNAPSHOT for tracked tokens, runs three independent
detectors per token, and re-emits typed signal events:

  EVT_BOOK_IMBALANCE       — sustained directional pressure
  EVT_SPOOF_WALL_DETECTED  — large limit removed without fill
  EVT_LIQUIDITY_GAP        — thin top-of-book; first deep level N+ ticks away
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from poly_terminal.agents.orderbook_intel.imbalance import (
    ImbalanceConfig,
    ImbalanceDetector,
)
from poly_terminal.agents.orderbook_intel.walls import (
    SpoofWallDetector,
    WallConfig,
)
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_BOOK_IMBALANCE,
    EVT_BOOK_SNAPSHOT,
    EVT_LIQUIDITY_GAP,
    EVT_SPOOF_WALL_DETECTED,
)
from poly_terminal.data.clob.orderbook import (
    BookSnapshot,
    liquidity_gap_ticks,
)

logger = logging.getLogger(__name__)


class OrderbookIntelAgent:
    def __init__(
        self,
        bus: EventBus,
        imbalance_cfg: ImbalanceConfig | None = None,
        wall_cfg: WallConfig | None = None,
        gap_min_size_usd: Decimal = Decimal("500"),
        gap_min_ticks: int = 5,
        tick_size: Decimal = Decimal("0.01"),
    ) -> None:
        self._bus = bus
        self._imbalance_cfg = imbalance_cfg or ImbalanceConfig()
        self._wall_cfg = wall_cfg or WallConfig()
        self._gap_min_usd = gap_min_size_usd
        self._gap_min_ticks = gap_min_ticks
        self._tick_size = tick_size
        # Per-token detector instances (state is per-token).
        self._imbalance: dict[str, ImbalanceDetector] = {}
        self._walls: dict[str, SpoofWallDetector] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_BOOK_SNAPSHOT, self._on_snapshot)
        self._started = True

    async def _on_snapshot(self, _event: str, payload: Any) -> None:
        if not isinstance(payload, BookSnapshot):
            return
        token = payload.token_id
        if not token:
            return
        await self._run_imbalance(token, payload)
        await self._run_walls(token, payload)
        await self._run_gap(token, payload)

    async def _run_imbalance(self, token: str, book: BookSnapshot) -> None:
        det = self._imbalance.get(token)
        if det is None:
            det = ImbalanceDetector(cfg=self._imbalance_cfg)
            self._imbalance[token] = det
        sig = det.observe(book)
        if sig is not None:
            await self._bus.publish(EVT_BOOK_IMBALANCE, sig)

    async def _run_walls(self, token: str, book: BookSnapshot) -> None:
        det = self._walls.get(token)
        if det is None:
            det = SpoofWallDetector(cfg=self._wall_cfg)
            self._walls[token] = det
        sig = det.observe(book)
        if sig is not None:
            await self._bus.publish(EVT_SPOOF_WALL_DETECTED, sig)

    async def _run_gap(self, token: str, book: BookSnapshot) -> None:
        for side in ("bid", "ask"):
            gap = liquidity_gap_ticks(
                book,
                side=side,  # type: ignore[arg-type]
                min_size_usd=self._gap_min_usd,
                tick_size=self._tick_size,
            )
            if gap is None:
                continue
            if gap >= self._gap_min_ticks:
                await self._bus.publish(
                    EVT_LIQUIDITY_GAP,
                    {
                        "token_id": token,
                        "side": side,
                        "gap_ticks": gap,
                        "ts": book.ts,
                    },
                )
