"""Signal calibration harness for the crypto_bar_momentum strategy.

Phase 37 follow-up (2026-05-11). Built per the rebuild plan §7's
"signal calibration" prerequisite — before the crypto_bar_momentum
strategy can be promoted from scaffold to PAPER soak, we need
evidence that at least one candidate signal function produces
positive expectancy on historical Polymarket crypto bars.

This module is the offline tool that produces that evidence.

**What it does:**

  1. Take a list of historical ticks for a single token (price + size
     + timestamp), plus a `bar_close_ts` defining when the bar resolves.
  2. Walk forward in time. At each tick within the entry-TTC band, ask
     the candidate signal function for a score in [-1, 1].
  3. If the score crosses the threshold, simulate a hypothetical entry
     (BUY YES on positive score, BUY NO on negative).
  4. Track each entry through to bar-close. Compute realized PnL using
     the last tick's price as the bar-close proxy.
  5. Aggregate: n_wins, n_losses, total PnL, win rate.

**What it does NOT do:**

  * Replay actual on-chain resolution (we don't have per-bar
    resolution oracles in the local DB).
  * Account for slippage / fees / partial fills — those need a
    separate fill_simulator layer.
  * Compare multiple signals automatically. The CLI runs ONE signal
    at a time; comparison is the operator's job until we add it.

**Signal functions provided (reference set):**

  * `price_momentum_signal(ticks, now_ts, lookback_s)` — signed
    change in price over the lookback window, normalized to [-1, 1].
  * `vwap_deviation_signal(ticks, now_ts, lookback_s)` — sign of
    (current price - VWAP over lookback).

Both intentionally use ONLY (price, size, ts) — matches the shape
of `research_orderbook_ticks` rows in the local DB.

Usage:

    from poly_terminal.research.signal_calibration_harness import (
        run_harness, price_momentum_signal, HarnessConfig,
    )
    ticks = load_ticks_for_token(token_id, since_ts, until_ts)
    result = run_harness(
        ticks,
        signal_fn=price_momentum_signal,
        cfg=HarnessConfig(bar_close_ts=...),
    )
    print(result.summary())
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol


@dataclass(frozen=True)
class HistoricalTick:
    """One row from `research_orderbook_ticks` — price + size + ts."""
    ts: int
    price: float
    size: float = 0.0


@dataclass(frozen=True)
class HypotheticalEntry:
    """A simulated entry produced by the harness when a signal fires."""
    entry_ts: int
    direction: str          # "YES" or "NO"
    entry_price: float
    exit_price: float
    realized_pnl: float     # (exit - entry) on YES, (entry - exit) on NO
    signal_score: float


@dataclass
class HarnessResult:
    """Aggregate output of a single harness run."""
    signals_evaluated: int
    entries: list[HypotheticalEntry]

    @property
    def n_wins(self) -> int:
        return sum(1 for e in self.entries if e.realized_pnl > 0)

    @property
    def n_losses(self) -> int:
        return sum(1 for e in self.entries if e.realized_pnl < 0)

    @property
    def win_rate(self) -> float:
        n = len(self.entries)
        return self.n_wins / n if n else 0.0

    @property
    def total_pnl(self) -> float:
        return round(sum(e.realized_pnl for e in self.entries), 6)

    def summary(self) -> str:
        return (
            f"signals_evaluated={self.signals_evaluated}, "
            f"entries={len(self.entries)}, "
            f"wins={self.n_wins} losses={self.n_losses}, "
            f"win_rate={self.win_rate:.1%}, "
            f"total_pnl=${self.total_pnl:+.4f}"
        )


@dataclass(frozen=True)
class HarnessConfig:
    """Tunable parameters for one harness run."""
    bar_close_ts: int                  # when the bar resolves (Unix s)
    signal_threshold: float = 0.5       # |score| ≥ this → enter
    entry_ttc_min_s: int = 5            # closest to close we'll enter
    entry_ttc_max_s: int = 60           # furthest from close we'll enter
    lookback_s: int = 10                # signal's window size


class SignalFunction(Protocol):
    """Signal contract: given the ticks visible at `now_ts` and a
    lookback window, return a score in [-1, 1]. Positive → BUY YES;
    Negative → BUY NO; near-zero → no entry."""
    def __call__(
        self, ticks: list[HistoricalTick], now_ts: int, *,
        lookback_s: int = 10,
    ) -> float: ...


# ─────────────────────── reference signal functions ───────────────────
def price_momentum_signal(
    ticks: list[HistoricalTick],
    now_ts: int,
    *,
    lookback_s: int = 10,
) -> float:
    """Signed price change over the lookback window, normalized.

    Score = (price_now - price_lookback_ago) × 20.0 clamped to [-1, 1].
    The ×20 scaling means a 5-cent move maps to score ±1.0 — picked
    so the threshold of ±0.5 corresponds to a 2.5-cent move, which is
    meaningful on 5-min Polymarket bars where prices typically move
    1-3 cents per minute.
    """
    if not ticks:
        return 0.0
    # Find the last tick at-or-before now_ts (current price)
    current = None
    for t in reversed(ticks):
        if t.ts <= now_ts:
            current = t
            break
    if current is None:
        return 0.0
    # Find the last tick at-or-before (now_ts - lookback_s)
    cutoff = now_ts - lookback_s
    past = None
    for t in reversed(ticks):
        if t.ts <= cutoff:
            past = t
            break
    if past is None:
        # No tick in the lookback window → 0 (don't extrapolate).
        return 0.0
    delta = current.price - past.price
    score = delta * 20.0
    return max(-1.0, min(1.0, score))


def last_stretch_band_signal(
    ticks: list[HistoricalTick],
    now_ts: int,
    *,
    lookback_s: int = 10,
    price_lo: float = 0.95,
    price_hi: float = 0.99,
) -> float:
    """Last Stretch Farming signal — returns +1.0 when the current
    price is in `[price_lo, price_hi]`, else 0.0.

    Used by `scripts/backtest/signal_calibration.py` to back-test the
    naked price-band hypothesis: "buy any YES side at 0.95+ in the
    last 90s of a bar". The historical empirical answer (367 positions
    in our DB, -1.42% ROI) said no, but this signal lets future
    operators rerun against larger samples as they accumulate.

    Phase 40 (2026-05-12) — research signal only.
    """
    if not ticks:
        return 0.0
    current = None
    for t in reversed(ticks):
        if t.ts <= now_ts:
            current = t
            break
    if current is None:
        return 0.0
    return 1.0 if price_lo <= current.price <= price_hi else 0.0


def vwap_deviation_signal(
    ticks: list[HistoricalTick],
    now_ts: int,
    *,
    lookback_s: int = 10,
) -> float:
    """Signed deviation of current price from the volume-weighted
    average price over the lookback window.

    Score = (price_now - VWAP) × 20.0 clamped to [-1, 1]. Same scale
    as `price_momentum_signal` so thresholds are comparable.
    """
    if not ticks:
        return 0.0
    current = None
    for t in reversed(ticks):
        if t.ts <= now_ts:
            current = t
            break
    if current is None:
        return 0.0
    cutoff = now_ts - lookback_s
    window = [t for t in ticks if cutoff <= t.ts <= now_ts]
    if not window:
        return 0.0
    total_vol = sum(t.size for t in window)
    if total_vol <= 0:
        return 0.0
    vwap = sum(t.price * t.size for t in window) / total_vol
    delta = current.price - vwap
    score = delta * 20.0
    return max(-1.0, min(1.0, score))


# ─────────────────────────────── harness ──────────────────────────────
def run_harness(
    ticks: list[HistoricalTick],
    *,
    signal_fn: SignalFunction | Callable[..., float],
    cfg: HarnessConfig,
) -> HarnessResult:
    """Walk forward through `ticks`, evaluating `signal_fn` at each
    tick whose `ts` falls within the entry-TTC band relative to
    `cfg.bar_close_ts`. Emit a hypothetical entry whenever |score|
    crosses `cfg.signal_threshold`.

    Each entry's exit price is the LAST tick's price (the bar-close
    proxy). Realized PnL is exit - entry for YES, entry - exit for NO.
    """
    if not ticks:
        return HarnessResult(signals_evaluated=0, entries=[])
    # Sort once; downstream signal fns assume monotonic ts.
    sorted_ticks = sorted(ticks, key=lambda t: t.ts)
    # Bar resolves at the last available tick's price (best proxy
    # without a separate resolution oracle).
    bar_close_price = sorted_ticks[-1].price

    entries: list[HypotheticalEntry] = []
    signals_evaluated = 0
    # We walk each tick's ts as a candidate entry timestamp. For each:
    #   * Check TTC band relative to bar_close_ts
    #   * Compute signal using ticks strictly at-or-before this ts
    #   * If |score| >= threshold, simulate entry
    for t in sorted_ticks:
        ttc = cfg.bar_close_ts - t.ts
        if not (cfg.entry_ttc_min_s <= ttc <= cfg.entry_ttc_max_s):
            continue
        # Ticks visible at decision time
        visible = [h for h in sorted_ticks if h.ts <= t.ts]
        score = signal_fn(visible, t.ts, lookback_s=cfg.lookback_s)
        signals_evaluated += 1
        if abs(score) < cfg.signal_threshold:
            continue
        direction = "YES" if score > 0 else "NO"
        entry_price = t.price
        if direction == "YES":
            realized = bar_close_price - entry_price
        else:
            realized = entry_price - bar_close_price
        entries.append(
            HypotheticalEntry(
                entry_ts=int(t.ts),
                direction=direction,
                entry_price=round(float(entry_price), 6),
                exit_price=round(float(bar_close_price), 6),
                realized_pnl=round(float(realized), 6),
                signal_score=round(float(score), 6),
            ),
        )
    return HarnessResult(
        signals_evaluated=signals_evaluated, entries=entries,
    )
