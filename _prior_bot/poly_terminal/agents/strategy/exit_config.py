"""Per-strategy exit parameters.

Default values pinned in docs/02_V3_ARCHITECTURE.md §2.7. The single
`ExitDecisionEngine` consumes these — strategies do not implement their
own SL/TP. See ADR 0002.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Final


@dataclass(frozen=True)
class ExitConfig:
    """Frozen exit parameters per strategy.

    Compose three orthogonal triggers (percent + dollar floor + tick
    confirmation) with a time stop fallback. SL fires only when ALL three
    conditions agree; TP fires on percent alone (no tick confirmation
    needed because we're locking gains).
    """

    sl_pct: Decimal = Decimal("0.15")
    tp_pct: Decimal = Decimal("0.25")
    sl_floor_usd: Decimal = Decimal("0.50")
    # 2026-05-08 PHASE 27 — adaptive trailing stop-loss.
    # `sl_arm_pct`: peak unrealized must reach `entry × (1 + sl_arm_pct)`
    #     before the SL switches from entry-relative fixed to peak-
    #     relative trailing. Default 0 = disabled (legacy fixed-SL only).
    # `sl_trail_pct`: when armed, trailing floor = peak × (1 - sl_trail_pct).
    #     Tick at-or-below the floor → EXIT_SL_TRAIL.
    # Captures the v44 case where the bot wanted to exit on a stale tick
    # but the actual book had recovered — and the more general case
    # where positions round-trip back through entry without locking
    # any of the favorable move. Set sl_arm_pct = 0 to keep legacy
    # fixed-SL behavior (used by the 9-RED test suite).
    sl_arm_pct: Decimal = Decimal("0")
    sl_trail_pct: Decimal = Decimal("0.05")
    # 2026-05-08 PHASE 28 — patient SELL on EXIT_SL.
    # Operator observation (v44): the bot exits at SL by crossing the
    # bid (FAK), but the user manually noticed price often recovers —
    # placing a GTC SELL at a scalp price (typically `last_trade_price`)
    # captures the upside instead of dumping at the bid.
    #
    # When `sl_patient_mode=True` AND there's enough time before bar
    # resolution (`time_to_close > sl_patient_min_time_to_close_s`),
    # the EXIT_SL flow:
    #   1. Submits GTC SELL at `sl_patient_target` price
    #      ("last_trade", "best_ask", or "midpoint")
    #   2. Polls order status for `sl_patient_wait_s` seconds
    #   3. If filled → success (better price + maker rebate)
    #   4. If not filled → cancel + fall through to legacy FAK escalation
    #
    # Default: OFF. Operator opts in via env (SL_PATIENT_MODE=true) so
    # the behavior change is reviewable in PAPER / LIVE_DRY first.
    # Per-strategy can also opt in via this field.
    sl_patient_mode: bool = False
    sl_patient_wait_s: int = 30
    sl_patient_target: str = "last_trade"  # "last_trade" | "best_ask" | "midpoint"
    sl_patient_min_time_to_close_s: int = 600  # skip if bar resolves <10min
    # 2026-05-08 PHASE 24.5 — TP dollar floor.
    # Mirrors `sl_floor_usd` on the take-profit side. TP fires only
    # when BOTH `pct_move >= tp_pct` AND `unrealized_usd >= tp_floor_usd`.
    # Filters out tiny gains on small positions where the +5% pct
    # threshold falls inside the on-chain round-trip drag (~$0.20
    # observed). Default 0 disables the floor (backward compat with
    # the legacy 9-RED test suite); per-strategy configs opt in at
    # ≥ $0.40 to cover drag with a 100% safety margin.
    tp_floor_usd: Decimal = Decimal("0")
    adverse_ticks_required: int = 2
    max_hold_seconds: int = 300
    # 2026-05-05: warmup window after position open during which TP/SL
    # branches are suppressed. PositionState.entry_price is set from
    # `intent.limit_price` (NOT the actual fill price), so the first
    # tick — whether from REST poll or the WS `last_trade_price` event —
    # can show a -50% "drop" that's actually just our limit being above
    # the prior trade. Without this window, every position on an
    # illiquid market exits within seconds with a fake SL fire. TIME
    # (max_hold) is checked before warmup, so this only gates TP/SL.
    #
    # Default is 0 (no warmup) — preserves backward compat with the
    # 9-RED test suite + restored positions whose entry_ts is already
    # well past any sensible warmup. Production per-strategy configs
    # set this to 30+.
    min_evaluation_age_s: int = 0
    # 2026-05-05 (deep-research-23 item #5): time-remaining-aware
    # warmup. For short-expiry markets a flat 30s warmup is too long
    # — a 5-minute binary spends 10% of its life inside the gate.
    # When this fraction is > 0 AND the position has an end_date_iso,
    # the effective warmup is `min(min_evaluation_age_s, frac *
    # remaining_at_entry)`. Example: 5% on a 5-min market with 5min
    # remaining at entry = 15s warmup; on a 24h market = 4320s, but
    # the static 30s floor wins so the actual warmup is 30s.
    #
    # Default 0 disables the dynamic adjustment so existing tests +
    # restored positions keep the static behaviour. Per-strategy
    # configs opt in.
    min_evaluation_age_pct_of_remaining: float = 0.0


EXIT_CONFIGS: Final[dict[str, ExitConfig]] = {
    "copy_trade": ExitConfig(
        # 2026-05-08 PHASE 24 retune. Backtest of 97 closed positions
        # today showed +5% TP would have produced $+8.55 cumulative
        # vs actual $+4.10 (+108% PnL improvement). 57/97 positions
        # touched +5% at some point — many crashed back to SL after a
        # transient peak. Earlier TP locks more wins.
        # Re-evaluate at 8% / 10% in the next cron review.
        #
        # Pre-Phase-24: tp_pct=0.15, sl_pct=0.10 (Phase 19 retune).
        # SL stays at 10% — the same backtest showed tighter SL net-
        # negative (-$2.04 to -$2.31) and looser SL even worse (-$7.36).
        sl_pct=Decimal("0.10"),
        tp_pct=Decimal("0.05"),
        sl_floor_usd=Decimal("0.50"),
        # 2026-05-08 PHASE 24.5: drag-aware floor. User-observed
        # ~$0.20 fixed cost per round-trip (gas + relayer + spread).
        # 5% TP on a $1.50 position fires at +$0.075 — net of drag
        # is -$0.125. The $0.40 floor blocks those traps; TP requires
        # BOTH pct AND dollar gates.
        tp_floor_usd=Decimal("0.40"),
        # 2026-05-08 PHASE 27 — patience SL.
        # Bumped from 1 → 2 so a single transient down-tick can't fire
        # SL alone. Combined with `sl_arm_pct` below, gives positions
        # a chance to bounce before the bot panics out.
        # v44 review: pos 22473 fired EXIT_SL_ABS on a stale $0.56 tick
        # but the actual book had recovered to $0.83 — the bot wanted
        # to exit on data that didn't reflect current book state. More
        # tick confirmation reduces single-stale-tick exits.
        adverse_ticks_required=2,
        # 2026-05-08 PHASE 27 — peak-trailing SL.
        # Once peak unrealized rises above +2% on cost, switch from
        # entry-relative fixed SL (-7%) to peak-relative trailing SL
        # (-5% from peak). Locks gains that would otherwise round-trip
        # back to fixed-SL territory.
        sl_arm_pct=Decimal("0.02"),
        sl_trail_pct=Decimal("0.05"),
        max_hold_seconds=86_400,  # 24h fallback
        min_evaluation_age_s=30,  # 2026-05-05 limit-vs-fill warmup
        # 2026-05-06 LIVE canary incident — copy_trade copies into
        # WHATEVER market the leader trades, including 5-minute
        # Polymarket "Up/Down 5m" micro-markets. Pos 22318 was a
        # SOL Up/Down with 26s remaining at entry; the 30s static
        # warmup blocked 54/56 ticks (44 of which were ≥ TP15).
        # Peak was $0.10 (10× return on $0.01 entry). Market then
        # resolved DOWN → -$2.23 realized, missing +$18.95 of upside.
        #
        # Now opted into the dynamic cap (matches every other
        # strategy except the original copy_trade default). For a
        # 24h max_hold the static 30s still wins; for short bars
        # the dynamic kicks in.
        #   24h bar  → 5% × 86400 = 4320s, static 30s wins
        #   5m bar   → 5% × 300   = 15s,   dynamic wins
        #   1m bar   → 5% × 60    = 3s,    dynamic wins
        #   26s bar  → 5% × 26    = 1.3s,  dynamic wins (May 6 case)
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    "flash_crash": ExitConfig(
        sl_pct=Decimal("0.12"),
        tp_pct=Decimal("0.20"),
        sl_floor_usd=Decimal("0.40"),
        adverse_ticks_required=2,
        max_hold_seconds=300,  # 5min
        min_evaluation_age_s=30,
        # 5min market: 5% × 300s = 15s — half the static 30s floor.
        # Per Item #5 we want shorter warmup for short bars.
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    "scalp_15m": ExitConfig(
        sl_pct=Decimal("0.10"),
        tp_pct=Decimal("0.18"),
        sl_floor_usd=Decimal("0.30"),
        adverse_ticks_required=2,
        max_hold_seconds=720,  # 12min
        min_evaluation_age_s=30,
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    "scalp_1h": ExitConfig(
        sl_pct=Decimal("0.12"),
        tp_pct=Decimal("0.22"),
        sl_floor_usd=Decimal("0.40"),
        adverse_ticks_required=2,
        max_hold_seconds=3_000,  # 50min
        min_evaluation_age_s=30,
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    "dump_hedge": ExitConfig(
        sl_pct=Decimal("0.08"),
        tp_pct=Decimal("0.12"),
        sl_floor_usd=Decimal("0.50"),
        adverse_ticks_required=1,
        max_hold_seconds=900,  # 15min
        min_evaluation_age_s=30,
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    # 2026-05-09 PHASE 32 P3 — Endgame Yield exit profile.
    # Buys near-certain outcomes converging toward 1.00. Tight TP at
    # ~target_exit (covered by the strategy's own exit price gate);
    # SL is shallow because the EV math says a >2% drawdown invalidates
    # the entry confidence. Long max_hold to give the convergence
    # time, but bounded by the strategy's `time_to_close_max_s` (6h).
    "endgame_yield": ExitConfig(
        sl_pct=Decimal("0.04"),
        tp_pct=Decimal("0.05"),
        sl_floor_usd=Decimal("0.10"),
        tp_floor_usd=Decimal("0.10"),
        adverse_ticks_required=3,  # convergence is slow; tolerate noise
        max_hold_seconds=21600,    # 6h — matches endgame ttc cap
        min_evaluation_age_s=60,
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    "copy_scalp_active": ExitConfig(
        # Identical exit profile to copy_scalp — independent name for separate
        # fill attribution between the legacy cohort and the leaderboard cohort.
        sl_pct=Decimal("0.07"),
        tp_pct=Decimal("0.05"),
        sl_floor_usd=Decimal("0.20"),
        tp_floor_usd=Decimal("0.40"),
        adverse_ticks_required=2,
        sl_arm_pct=Decimal("0.02"),
        sl_trail_pct=Decimal("0.05"),
        max_hold_seconds=600,
        min_evaluation_age_s=30,
        min_evaluation_age_pct_of_remaining=0.05,
    ),
    "copy_scalp": ExitConfig(
        # Scalp profile for wallet-signal entries: tight TP/SL,
        # short max_hold so positions exit before the next signal.
        # Aimed at high-frequency wallets where the alpha decays in
        # minutes, not hours. SL slightly tighter than copy_trade
        # because scalp size is smaller — we can't afford 10% drawdown
        # on a $3 position to be a meaningful loss.
        #
        # 2026-05-08 PHASE 24: aligned tp_pct with copy_trade at +5%
        # (was +10%). Same backtest argument applies — short bursts of
        # +5% are far more common than +10%, and locking quick wins
        # is the dominant edge for both copy strategies.
        sl_pct=Decimal("0.07"),
        tp_pct=Decimal("0.05"),
        sl_floor_usd=Decimal("0.20"),
        # 2026-05-08 PHASE 24.5: same drag floor as copy_trade.
        # copy_scalp's smaller positions make the floor MORE
        # important, not less.
        tp_floor_usd=Decimal("0.40"),
        # 2026-05-08 PHASE 27 — same patience + trailing-SL contract
        # as copy_trade. The shorter max_hold (10min) means trailing
        # has fewer ticks to work with, but the same logic applies.
        adverse_ticks_required=2,
        sl_arm_pct=Decimal("0.02"),
        sl_trail_pct=Decimal("0.05"),
        max_hold_seconds=600,  # 10min
        min_evaluation_age_s=30,  # 2026-05-05 limit-vs-fill warmup
        # 10min max_hold; if entry happens with 10min remaining a
        # 5% pct gives 30s — same as static. With 3min remaining
        # (late entry) → 9s warmup, lets fast scalps fire sooner.
        min_evaluation_age_pct_of_remaining=0.05,
    ),
}


def for_strategy(name: str) -> ExitConfig:
    """Return the registered ExitConfig for `name`, or safe defaults."""
    return EXIT_CONFIGS.get(name, ExitConfig())
