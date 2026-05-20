"""Confirmed peak detector — Bug #2 root fix.

A peak counts only when:
  1. Buffer length >= `min_buffer_bars`.
  2. Peak bar is at least `min_age_bars` old.
  3. >= `confirmation_bars` adjacent bars are within `peak_proximity_bps`
     of the peak price.

`should_fire(current_price)` then composes:
  4. Drop from confirmed peak >= `drop_threshold_pct`.
  5. Current bar's size >= `size_surge_multiplier` × rolling mean.
All three filters must agree.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class Bar:
    ts: float
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class PeakConfig:
    min_buffer_bars: int = 30
    confirmation_bars: int = 2
    peak_proximity_bps: int = 100               # 1% by default
    min_age_bars: int = 3
    drop_threshold_pct: Decimal = Decimal("0.12")
    size_surge_multiplier: Decimal = Decimal("3.0")
    buffer_capacity: int = 200


@dataclass
class ConfirmedPeakDetector:
    cfg: PeakConfig = field(default_factory=PeakConfig)
    _buf: deque[Bar] = field(init=False)

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=self.cfg.buffer_capacity)

    def add_bar(self, bar: Bar) -> None:
        self._buf.append(bar)

    def confirmed_peak(self) -> Decimal | None:
        if len(self._buf) < self.cfg.min_buffer_bars:
            return None
        bars = list(self._buf)
        peak_idx = max(range(len(bars)), key=lambda i: bars[i].price)
        peak = bars[peak_idx]
        # Peak must be old enough.
        now_idx = len(bars) - 1
        if now_idx - peak_idx < self.cfg.min_age_bars:
            return None
        # Peak must have N adjacent bars near it.
        proximity = peak.price * Decimal(self.cfg.peak_proximity_bps) / Decimal(10_000)
        adjacent = 0
        for offset in range(1, self.cfg.buffer_capacity):
            checked = False
            if peak_idx - offset >= 0:
                if abs(bars[peak_idx - offset].price - peak.price) <= proximity:
                    adjacent += 1
                checked = True
            if peak_idx + offset < len(bars):
                if abs(bars[peak_idx + offset].price - peak.price) <= proximity:
                    adjacent += 1
                checked = True
            if not checked:
                break
            if adjacent >= self.cfg.confirmation_bars:
                return peak.price
        return None

    def size_surge(self) -> bool:
        if len(self._buf) < self.cfg.min_buffer_bars:
            return False
        bars = list(self._buf)
        recent = bars[-1].size
        baseline_count = max(1, self.cfg.min_buffer_bars - 1)
        baseline_total = sum(
            (b.size for b in bars[-self.cfg.min_buffer_bars:-1]),
            start=Decimal("0"),
        )
        baseline = baseline_total / Decimal(baseline_count)
        if baseline <= 0:
            return False
        return recent >= baseline * self.cfg.size_surge_multiplier

    def should_fire(self, current_price: Decimal) -> bool:
        peak = self.confirmed_peak()
        if peak is None:
            return False
        drop = (peak - current_price) / peak
        if drop < self.cfg.drop_threshold_pct:
            return False
        if not self.size_surge():
            return False
        return True

    @property
    def buffer_size(self) -> int:
        return len(self._buf)
