"""Wallet conviction scoring.

Pure-function scorer over a list of `WalletHistoryRow`. Per
docs/02_V3_ARCHITECTURE.md §2.1:

  conviction_score =
        win_rate                       * w_win    (default 0.50)
      + log1p(max(0, avg_roi_pct))     * w_roi    (default 0.25)
      + log1p(trades_in_window)        * w_trades (default 0.15)
      - abs(median_pos - target)/target * w_size  (default 0.10)

Negative ROI is clamped at 0 in the log1p term so a negative-EV wallet
still scores low (via low win_rate) but the formula stays finite. The
size-penalty floor is also clamped at 0 so the score never goes below
the win-rate term alone.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import median

from poly_terminal.persistence.repositories.wallets import WalletHistoryRow


@dataclass(frozen=True)
class ScoreInputs:
    history: list[WalletHistoryRow]
    target_position_usd: float
    now_ts: int = 0
    window_days: int = 30

    def effective_now(self) -> int:
        return self.now_ts or int(datetime.now(timezone.utc).timestamp())


@dataclass(frozen=True)
class ScoreResult:
    win_rate: float
    avg_roi_pct: float
    trades_30d: int
    median_position_usd: float
    conviction_score: float


@dataclass(frozen=True)
class Scorer:
    w_win: float = 0.50
    w_roi: float = 0.25
    w_trades: float = 0.15
    w_size: float = 0.10

    @property
    def weights(self) -> tuple[float, float, float, float]:
        return (self.w_win, self.w_roi, self.w_trades, self.w_size)

    def score(self, inputs: ScoreInputs) -> ScoreResult:
        return _compute(inputs, self)


def compute_score(inputs: ScoreInputs, scorer: Scorer | None = None) -> ScoreResult:
    return _compute(inputs, scorer or Scorer())


def _compute(inputs: ScoreInputs, scorer: Scorer) -> ScoreResult:
    now = inputs.effective_now()
    window = inputs.window_days * 86_400
    cutoff = now - window
    recent = [
        h for h in inputs.history
        if h.closed_at is not None and h.closed_at >= cutoff
    ]
    if not recent:
        return ScoreResult(
            win_rate=0.0,
            avg_roi_pct=0.0,
            trades_30d=0,
            median_position_usd=0.0,
            conviction_score=0.0,
        )

    wins = sum(1 for h in recent if (h.pnl_usd or 0) > 0)
    win_rate = wins / len(recent)

    rois: list[float] = []
    for h in recent:
        if h.size_usd <= 0 or h.pnl_usd is None:
            continue
        rois.append(h.pnl_usd / h.size_usd)
    avg_roi = sum(rois) / len(rois) if rois else 0.0

    sizes = [h.size_usd for h in recent if h.size_usd > 0]
    med_size = median(sizes) if sizes else 0.0

    target = max(inputs.target_position_usd, 1e-6)
    size_penalty = abs(med_size - target) / target

    conviction = (
        win_rate * scorer.w_win
        + math.log1p(max(0.0, avg_roi)) * scorer.w_roi
        + math.log1p(len(recent)) * scorer.w_trades
        - size_penalty * scorer.w_size
    )
    if not math.isfinite(conviction):
        conviction = 0.0
    if conviction < 0.0:
        conviction = 0.0

    return ScoreResult(
        win_rate=float(win_rate),
        avg_roi_pct=float(avg_roi),
        trades_30d=int(len(recent)),
        median_position_usd=float(med_size),
        conviction_score=float(conviction),
    )
