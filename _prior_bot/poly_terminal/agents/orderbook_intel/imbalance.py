"""Sustained imbalance detector.

Pure-state class — feed snapshots to `observe()`, get an `ImbalanceSignal`
back when `|imbalance| ≥ threshold` for `≥ confirmation_bars` consecutive
snapshots on the same side.

Filters two failure modes:
  - Single-tick spike (one heavy snapshot followed by reversion) — no fire.
  - Repeated same-side fires — dedupes until the side flips or the imbalance
    drops below threshold.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from poly_terminal.data.clob.orderbook import (
    BookSnapshot,
    bid_ask_imbalance,
)

Side = Literal["BID", "ASK"]


@dataclass(frozen=True)
class ImbalanceConfig:
    threshold: Decimal = Decimal("0.30")
    confirmation_bars: int = 3
    top_n: int = 5


@dataclass(frozen=True)
class ImbalanceSignal:
    token_id: str
    side: Side
    value: Decimal           # absolute imbalance value
    bars: int                # how many consecutive bars
    ts: int


@dataclass
class ImbalanceDetector:
    cfg: ImbalanceConfig = field(default_factory=ImbalanceConfig)
    _streak_side: Side | None = None
    _streak_count: int = 0
    _emitted_for_streak: bool = False

    def observe(self, book: BookSnapshot) -> ImbalanceSignal | None:
        value = bid_ask_imbalance(book, top_n=self.cfg.top_n)
        if value is None:
            self._reset()
            return None
        side: Side | None
        if value > self.cfg.threshold:
            side = "BID"
        elif value < -self.cfg.threshold:
            side = "ASK"
        else:
            self._reset()
            return None
        if side != self._streak_side:
            self._streak_side = side
            self._streak_count = 1
            self._emitted_for_streak = False
        else:
            self._streak_count += 1
        if (
            self._streak_count >= self.cfg.confirmation_bars
            and not self._emitted_for_streak
        ):
            self._emitted_for_streak = True
            return ImbalanceSignal(
                token_id=book.token_id,
                side=side,
                value=abs(value),
                bars=self._streak_count,
                ts=book.ts,
            )
        return None

    def _reset(self) -> None:
        self._streak_side = None
        self._streak_count = 0
        self._emitted_for_streak = False
