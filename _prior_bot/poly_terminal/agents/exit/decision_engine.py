"""The single SL/TP/time-exit path — Bug #1 fix root.

Decision rules verbatim from ADR 0002 + docs/02_V3_ARCHITECTURE.md §2.7:

1. Whale-out (if signaled) → EXIT_WHALE_OUT
2. Time stop (cheapest, runs first if no whale signal) → EXIT_TIME
3. Take-profit (percent only, no tick confirmation) → EXIT_TP
4. Stop-loss compound:  pct AND dollar-floor AND tick-count → EXIT_SL
5. Otherwise → HOLD

Strategies do NOT implement their own SL — they emit BuyIntent with an
ExitConfig and the Exit Agent owns the watcher loop.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from poly_terminal.agents.exit.position_state import PositionState
from poly_terminal.agents.strategy.exit_config import ExitConfig
from poly_terminal.shared.enums import ExitDecision


# Block-reason strings for HOLD evaluations. Surfaced via
# `evaluate_with_reason` for the exit_evals observability layer
# (see persistence/repositories/exit_evals.py). Plain strings rather
# than an Enum to keep DB rows portable / debuggable.
BLOCK_WARMUP: str = "warmup"
# 2026-05-06 (Phase 3) — stale-init tick filter. Polymarket WS can
# deliver a price=$0.00 tick during the initial subscription handshake
# before real prices have loaded. Without filtering, that $0 tick
# fires SL immediately on any position with adverse_ticks_required=1
# (because the implied -100% drop is the maximum possible). Pos 22318
# (May 6 LIVE canary) saw exactly this scenario: 26-second SOL Up/Down
# market, first WS tick at $0.00, would have fired SL even AFTER the
# warmup-pct fix had we restarted with patched code. Filtering this
# class of tick at the engine boundary is the right fix because:
#   - profit_taker already does it (`if tick_price <= 0: return`)
#   - any downstream logic must be defensive against bad inputs
#   - the operator can see this filter firing in exit_evals.
BLOCK_INVALID_PRICE: str = "invalid_price"


@dataclass(frozen=True)
class ExitEvalResult:
    """Rich evaluation result. Exposed via `evaluate_with_reason()` so
    the exit observability layer can record both the decision and the
    reason a HOLD was returned (warmup vs no-trigger). Callers that
    only care about the control-flow decision should keep using
    `evaluate()` which returns just the `ExitDecision`."""

    decision: ExitDecision
    block_reason: str | None
    pct_move: Decimal
    unrealized_usd: Decimal


class ExitDecisionEngine:
    """Stateless evaluator. Mutates `PositionState.adverse_tick_count` in place."""

    def evaluate(
        self,
        pos: PositionState,
        current_price: Decimal,
        cfg: ExitConfig,
        now_ts: float,
        *,
        whale_out: bool = False,
    ) -> ExitDecision:
        """Backward-compatible wrapper around `evaluate_with_reason`.
        Returns the bare ExitDecision so existing callers + the legacy
        test suite keep working unchanged."""
        return self.evaluate_with_reason(
            pos, current_price, cfg, now_ts, whale_out=whale_out
        ).decision

    def evaluate_with_reason(
        self,
        pos: PositionState,
        current_price: Decimal,
        cfg: ExitConfig,
        now_ts: float,
        *,
        whale_out: bool = False,
    ) -> ExitEvalResult:
        """Same logic as `evaluate`, but returns an `ExitEvalResult`
        carrying decision + block_reason + computed metrics. Used by
        ExitAgent / ProfitTakerAgent for the exit_evals trace."""
        # 1. Whale-out — top-rank wallet sold; bail regardless of price.
        if whale_out:
            return ExitEvalResult(
                decision=ExitDecision.EXIT_WHALE_OUT,
                block_reason=None,
                pct_move=pos.pct_move(current_price),
                unrealized_usd=pos.unrealized_usd(current_price),
            )

        # 2. Time stop — cheapest check, runs first.
        if (now_ts - pos.entry_ts) >= cfg.max_hold_seconds:
            return ExitEvalResult(
                decision=ExitDecision.EXIT_TIME,
                block_reason=None,
                pct_move=pos.pct_move(current_price),
                unrealized_usd=pos.unrealized_usd(current_price),
            )

        # 2.4. Invalid-price filter (2026-05-06 Phase 3). Reject ticks
        # with price <= 0 BEFORE updating PositionState — otherwise the
        # bad price pollutes adverse_tick_count / last_price and the
        # next real tick fires SL on a phantom adverse run. profit_taker
        # already guards this path; the engine must too.
        if current_price <= 0:
            return ExitEvalResult(
                decision=ExitDecision.HOLD,
                block_reason=BLOCK_INVALID_PRICE,
                # Use Decimal("0") for both — the metrics are meaningless
                # on a $0 tick and we don't want them to look like real
                # signals in dashboards / exit_evals queries.
                pct_move=Decimal("0"),
                unrealized_usd=Decimal("0"),
            )

        # Update tick state regardless of which branch decides — observe
        # before any return so adverse runs are correctly counted.
        pos.observe_tick(current_price)

        # 2.5. Warmup window — skip TP/SL judgment during the initial
        # settle period. pos.entry_price is the limit price the bot
        # set, NOT the actual fill price; the first tick can show a
        # ±50% gap that's pure entry/fill drift, not market movement.
        # Filtering this window prevents fake SL fires on illiquid
        # markets. observe_tick still ran above so adverse_tick_count
        # accumulates accurately for the post-warmup window.
        # 2026-05-05 — added after TickPoller's REST fallback exposed
        # the latent issue; even WS-driven ticks had the same bug.
        #
        # Negative-elapsed handling (2026-05-05 follow-up): synthetic
        # tests publish ticks with simulated timestamps (e.g. ts=1.7B,
        # year 2023) while ExecutionAgent stamps entry_ts with
        # `time.time()` (e.g. 1.778B, year 2026). The naive
        # `(now_ts - pos.entry_ts) < 30` predicate then evaluates
        # `-78M < 30 → True` and blocks the exit forever. Treat
        # negative elapsed as "warmup not applicable" — the position
        # is older than the tick (clock skew or simulated time), so
        # there's no fresh-fill drift to filter. The legacy `> 0`
        # guard remains so cfg=0 short-circuits without computing
        # elapsed at all.
        # 2026-05-06 PHASE 6 — asymmetric warmup: TP fires immediately,
        # SL stays gated. The warmup was originally added (Phase 2) to
        # filter limit-vs-fill drift on SL fires — pos.entry_price was
        # the limit, not the actual fill, so the first tick at the
        # real market price could look like a fake drop. With Phase 4
        # deployed, pos.entry_price now reflects the ACTUAL fill (set
        # from `avg_fill_price` in the matched POST response), so a
        # recorded pct_move during warmup is no longer drift-distorted.
        # And BUYs always fill at-or-better than limit, so a recorded
        # pct_move >= tp_pct is a CONSERVATIVE estimate of actual gain.
        #
        # Production lesson — pos 22328 (May 6 canary v8) hit +15.4% at
        # +18s, was BLOCKED by warmup, then reverted to -7.7% which
        # fired SL at +31s. Net -$0.08 instead of locking +$0.10.
        # Phase 6 lets TP fire immediately when threshold is met.
        # SL still requires the warmup window to complete.
        pct_move = pos.pct_move(current_price)
        unrealized = pos.unrealized_usd(current_price)

        # 3. Take-profit FIRST (Phase 6) — no warmup gate, no tick
        # confirmation. This is the asymmetric early-fire path.
        # 2026-05-08 PHASE 24.5: TP also requires unrealized USD to
        # exceed `cfg.tp_floor_usd` (default 0 = disabled). Mirrors
        # the SL compound logic. Drag-aware filter — a 5% pct gate
        # on a $1.50 position fires at +$0.075 unrealized, which is
        # below the ~$0.20 round-trip drag. The dollar floor blocks
        # those net-losing fires.
        if pct_move >= cfg.tp_pct and unrealized >= cfg.tp_floor_usd:
            return ExitEvalResult(
                decision=ExitDecision.EXIT_TP,
                block_reason=None,
                pct_move=pct_move,
                unrealized_usd=unrealized,
            )

        # 3.5 Warmup window — gates SL and "no-trigger HOLD" only.
        # TP already short-circuited above. See the Phase 6 comment.
        if cfg.min_evaluation_age_s > 0:
            elapsed = now_ts - pos.entry_ts
            # 2026-05-05 (deep-research-23 item #5): dynamic warmup
            # bound by `min_evaluation_age_pct_of_remaining` × time
            # remaining at entry. For short-expiry markets a flat 30s
            # would consume 10% of a 5-min bar's life. The pct cap
            # gives short bars proportionally shorter warmups while
            # leaving long bars at the static floor. min() picks the
            # tighter of the two; pct=0 disables the cap.
            effective_warmup_s = float(cfg.min_evaluation_age_s)
            pct = cfg.min_evaluation_age_pct_of_remaining
            if pct > 0 and pos.bar_end_ts is not None:
                remaining_at_entry_s = pos.bar_end_ts - pos.entry_ts
                if remaining_at_entry_s > 0:
                    dynamic_cap = pct * remaining_at_entry_s
                    if dynamic_cap < effective_warmup_s:
                        effective_warmup_s = dynamic_cap
            if 0 <= elapsed < effective_warmup_s:
                return ExitEvalResult(
                    decision=ExitDecision.HOLD,
                    block_reason=BLOCK_WARMUP,
                    pct_move=pct_move,
                    unrealized_usd=unrealized,
                )

        # 4. Stop-loss — two paths:
        #
        # 4a. PHASE 27 trailing SL (2026-05-08). Once peak rises above
        #     `entry × (1 + sl_arm_pct)`, the SL switches from entry-
        #     relative fixed to peak-relative trailing. Captures the
        #     v44 case where positions round-trip through entry without
        #     locking gains. ALSO gated by `adverse_ticks_required` so a
        #     single stale tick below the floor can't fire — operator
        #     observation in v44: stale tick at $0.56 vs actual book at
        #     $0.83 would have fired SL prematurely. Set sl_arm_pct=0
        #     to disable trailing entirely (legacy fixed-SL only).
        sl_ticks_triggered = pos.adverse_tick_count >= cfg.adverse_ticks_required
        if cfg.sl_arm_pct > 0 and pos.peak_price > 0:
            arm_threshold = pos.entry_price * (
                Decimal("1") + cfg.sl_arm_pct
            )
            if pos.peak_price >= arm_threshold:
                trail_floor = pos.peak_price * (
                    Decimal("1") - cfg.sl_trail_pct
                )
                if current_price <= trail_floor and sl_ticks_triggered:
                    return ExitEvalResult(
                        decision=ExitDecision.EXIT_SL_TRAIL,
                        block_reason=None,
                        pct_move=pct_move,
                        unrealized_usd=unrealized,
                    )

        # 4b. Legacy fixed SL — all three conditions must agree.
        sl_pct_triggered = pct_move <= -cfg.sl_pct
        sl_floor_triggered = unrealized <= -cfg.sl_floor_usd
        sl_ticks_triggered = pos.adverse_tick_count >= cfg.adverse_ticks_required

        if sl_pct_triggered and sl_floor_triggered and sl_ticks_triggered:
            return ExitEvalResult(
                decision=ExitDecision.EXIT_SL,
                block_reason=None,
                pct_move=pct_move,
                unrealized_usd=unrealized,
            )

        return ExitEvalResult(
            decision=ExitDecision.HOLD,
            block_reason=None,
            pct_move=pct_move,
            unrealized_usd=unrealized,
        )
