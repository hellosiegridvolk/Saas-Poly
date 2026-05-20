"""ProfitTakerAgent — absolute-threshold exit.

Closes any open position whose unrealized PnL crosses
`profit_threshold_per_dollar` × cost_basis_usd. Default 0.10 = 10¢
per $1 of cost basis (= +10% on cost).

Independent of `ExitDecisionEngine`:
  - Fires on the very first qualifying tick — no
    `adverse_ticks_required` confirmation. The user wanted a
    guaranteed cashout the moment a position is in clean profit,
    not "wait for 2 ticks of confirmation that it's still up".
  - Uses `cost_basis_usd` (= entry_price × shares for BUY) as the
    denominator, not entry_price alone, so the threshold is a
    direct dollar-amount above what was paid.

Behavior:
  EVT_POSITION_OPENED  → start tracking position (cost_basis, shares,
                          token_id)
  EVT_MARKET_TICK      → for each open position on this token,
                          compute unrealized = (tick - entry) × shares
                          for a BUY; if unrealized >= threshold ×
                          cost_basis, fire EVT_SELL_INTENT with
                          reason="EXIT_TP_ABS".
  EVT_POSITION_CLOSED  → drop the position from tracking (the close
                          may have come from us, ExitAgent, or
                          BarResolutionWatcher — race-safe via the
                          PositionsRepo's idempotent close_position).

Race notes:
  - Both ExitDecisionEngine and ProfitTakerAgent can race to fire
    EVT_SELL_INTENT for the same position. ExecutionAgent's
    `_on_sell_intent` uses PositionsRepo.fetch_open then
    close_position, which returns None if the position was already
    closed — the second SELL no-ops cleanly.
  - We track an internal `_firing` set so the same position can't
    be re-fired by a second tick before the close lands.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_MARKET_TICK,
    EVT_POSITION_CLOSED,
    EVT_POSITION_OPENED,
    EVT_SELL_INTENT,
)
from poly_terminal.shared.enums import ExitDecision

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProfitTakerConfig:
    # 2026-05-08 PHASE 24 retune. Was 0.10 (+10% on cost). Backtest of
    # 97 closed positions today showed +5% fixed TP yields $+8.55
    # cumulative vs actual $+4.10 — +108% PnL improvement. The 5¢/$
    # default puts the dataclass at parity with the retuned per-strategy
    # ExitConfig.tp_pct = 0.05.
    # 5¢ per $1 of cost basis = +5% on cost. Set to 0 to disable.
    profit_threshold_per_dollar: Decimal = Decimal("0.05")
    # Symmetric loss-cut. 10¢ per $1 of cost basis = -10% on cost.
    # Same single-tick (no `adverse_ticks_required` confirmation)
    # behavior as the profit side, so a fast adverse move triggers
    # an immediate exit even before ExitDecisionEngine's tick logic.
    # Set to 0 to disable the loss side independently of the profit
    # side. (Loss threshold left at 10% — same backtest showed tighter
    # SL net-negative.)
    loss_threshold_per_dollar: Decimal = Decimal("0.10")
    # ── Trailing profit ───────────────────────────────────────────────
    # Once a position's unrealized PnL reaches `trail_arm_per_dollar`
    # × cost_basis, switch from fixed-threshold profit-take to
    # trailing mode: keep raising a "trail floor" as price rises;
    # when price retraces below the floor, fire EXIT_TP_TRAIL.
    #
    # The floor on each tick = max(
    #   peak_price - (peak_price - entry) × trail_giveback_pct,
    #   entry × (1 + trail_lock_pct)
    # )
    # i.e., give back at most `trail_giveback_pct` of the peak gain,
    # but never let the floor drop below `trail_lock_pct` above entry
    # (so we always lock SOME profit once trailing arms — never below
    # cost basis even if price crashes back).
    #
    # Set trail_arm_per_dollar=0 to disable trailing entirely (the
    # fixed profit_threshold_per_dollar takes over again).
    #
    # 2026-05-08 PHASE 24: trail_arm dropped from 0.10 to 0.05 to
    # track the new 5%-TP default — otherwise trailing would arm
    # ABOVE the fixed-TP threshold and never engage. trail_lock_pct
    # dropped from 0.05 to 0.03 so the locked floor stays meaningfully
    # below the arm point; trail_giveback unchanged.
    trail_arm_per_dollar: Decimal = Decimal("0.05")    # arm at +5% on cost
    trail_lock_pct: Decimal = Decimal("0.03")           # never let floor < +3%
    trail_giveback_pct: Decimal = Decimal("0.30")       # give back ≤ 30% of peak gain
    # 2026-05-08 PHASE 24.5 — TP dollar floor.
    # Drag-aware minimum on absolute USD gain. The effective TP_ABS
    # threshold becomes:
    #   max(profit_per_dollar × cost_basis, tp_floor_usd)
    # so on a $2 position a 5% legacy threshold of $0.10 is raised
    # to the floor, preventing net-losing TP fires. Trail-arm uses
    # the same floor for the same reason.
    #
    # Default 0 = disabled (backward compat with unit tests that
    # exercise small-position TP semantics). Production sites
    # (main.py) override with $0.40 to cover the user-observed
    # ~$0.20 round-trip drag with a 100% safety margin. Mirrors the
    # opt-in pattern used by ExitConfig.tp_floor_usd.
    tp_floor_usd: Decimal = Decimal("0")
    # 2026-05-05: warmup window mirroring ExitConfig.min_evaluation_age_s.
    # Position.entry_price is the limit price the bot SET (not the
    # actual fill); the first tick can show ±50% drift that's pure
    # entry-vs-fill mismatch, not market movement. Suppress profit/
    # loss-threshold fires for this window. 0 disables (legacy
    # behavior preserved for tests). Production builds set 30+.
    min_evaluation_age_s: int = 30


@dataclass
class _Tracked:
    position_id: int
    token_id: str
    side: str            # 'BUY' (only side currently emitted)
    entry_price: Decimal
    shares: Decimal
    cost_basis_usd: Decimal
    strategy: str
    # 2026-05-05: open timestamp (seconds since epoch) — needed for the
    # warmup gate in _on_tick. Defaults to 0 so legacy callers + tests
    # that omit entry_ts in the payload still work (combined with the
    # cfg.min_evaluation_age_s > 0 guard, default-0 entry_ts is treated
    # as "no warmup applies").
    entry_ts: float = 0.0
    # Trailing state. trail_armed flips True the first tick that
    # crosses cfg.trail_arm_per_dollar × cost_basis. Once armed,
    # peak_price tracks the highest tick seen and the trailing
    # exit fires when tick drops below the dynamic floor.
    trail_armed: bool = False
    peak_price: Decimal = Decimal("0")


class ProfitTakerAgent:
    def __init__(
        self,
        bus: EventBus,
        cfg: ProfitTakerConfig | None = None,
        eval_recorder: "Any | None" = None,  # ExitEvalsRepo, typed loosely
    ) -> None:
        self._bus = bus
        self._cfg = cfg or ProfitTakerConfig()
        self._open: dict[int, _Tracked] = {}
        self._by_token: dict[str, set[int]] = {}
        # Race lock: positions for which we've already published a
        # SELL_INTENT this lifecycle, to dedupe between rapid ticks.
        self._firing: set[int] = set()
        self._started = False
        # 2026-05-05: optional per-evaluation observability sink.
        # Mirrors ExitAgent — every tick eval records one row to
        # `exit_evals` so post-incident drilldown can answer 'did
        # warmup block ProfitTaker from firing?' and 'what was the
        # last unrealized USD before this position closed by TIME?'
        self._eval_recorder = eval_recorder
        # Stats — surfaced via /api/profit_taker (future) and tests.
        self.stats = {
            "ticks_observed": 0,
            "exits_fired": 0,
            "positions_tracked_high_water": 0,
            "trail_armed": 0,         # cumulative arms
            "trail_exits": 0,         # EXIT_TP_TRAIL fires
        }

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_POSITION_OPENED, self._on_open)
        self._bus.subscribe(EVT_POSITION_CLOSED, self._on_closed)
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)
        self._started = True

    @property
    def open_count(self) -> int:
        return len(self._open)

    @property
    def cfg(self) -> ProfitTakerConfig:
        return self._cfg

    def set_thresholds(
        self,
        profit_threshold_per_dollar: Decimal,
        loss_threshold_per_dollar: Decimal,
    ) -> None:
        """Hot-swap the profit/loss thresholds without restarting.
        Used by AutoTunerAgent to adapt to rolling PnL.

        ProfitTakerConfig is frozen, so we replace the whole object
        with the new values. The tracker map and firing-state set
        are untouched — only the comparison thresholds change.
        """
        self._cfg = ProfitTakerConfig(
            profit_threshold_per_dollar=profit_threshold_per_dollar,
            loss_threshold_per_dollar=loss_threshold_per_dollar,
        )

    # ── Position lifecycle ───────────────────────────────────────────

    async def _on_open(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            position_id = int(payload["position_id"])
            token_id = str(payload["token_id"])
            side = str(payload.get("side", "BUY")).upper()
            entry_price = Decimal(str(payload.get("entry_price", 0)))
            shares = Decimal(str(payload.get("shares", 0)))
            cost_basis = Decimal(str(payload.get("cost_basis_usd", 0)))
            strategy = str(payload.get("strategy", ""))
            # 2026-05-05: capture entry_ts for the warmup gate. Default
            # to 0 if the payload doesn't carry one — combined with the
            # `entry_ts > 0` predicate in _on_tick, that suppresses the
            # warmup check (preserves test backward-compat).
            entry_ts = float(payload.get("entry_ts", 0))
        except (KeyError, TypeError, ValueError):
            return
        if entry_price <= 0 or shares <= 0 or cost_basis <= 0:
            return
        # Only BUY positions track (SELL/short would invert the math —
        # v3 only opens BUYs today; gracefully ignore otherwise).
        if side != "BUY":
            return
        self._open[position_id] = _Tracked(
            position_id=position_id,
            token_id=token_id,
            side=side,
            entry_price=entry_price,
            shares=shares,
            cost_basis_usd=cost_basis,
            strategy=strategy,
            entry_ts=entry_ts,
        )
        self._by_token.setdefault(token_id, set()).add(position_id)
        self.stats["positions_tracked_high_water"] = max(
            self.stats["positions_tracked_high_water"], len(self._open)
        )

    async def _on_closed(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            position_id = int(payload["position_id"])
        except (KeyError, TypeError, ValueError):
            return
        tracked = self._open.pop(position_id, None)
        if tracked is None:
            return
        token_set = self._by_token.get(tracked.token_id)
        if token_set is not None:
            token_set.discard(position_id)
            if not token_set:
                self._by_token.pop(tracked.token_id, None)
        # Allow a re-opened position with the same id (extremely unlikely
        # given autoincrement PKs) to fire again.
        self._firing.discard(position_id)

    # ── Tick handler ─────────────────────────────────────────────────

    async def _on_tick(self, _e: str, payload: Any) -> None:
        self.stats["ticks_observed"] += 1
        if not isinstance(payload, dict):
            return
        # Both sides disabled = nothing to do.
        if (
            self._cfg.profit_threshold_per_dollar <= 0
            and self._cfg.loss_threshold_per_dollar <= 0
        ):
            return
        token_id = str(payload.get("token_id", ""))
        if not token_id:
            return
        position_ids = self._by_token.get(token_id)
        if not position_ids:
            return
        try:
            tick_price = Decimal(str(payload.get("price", 0)))
        except (TypeError, ValueError):
            return
        if tick_price <= 0:
            return
        # 2026-05-05: tick ts (seconds) for warmup gate. Bus contract is
        # seconds since 2026-05-05 normalization; default to 0 if
        # missing (warmup will short-circuit).
        try:
            tick_ts = float(payload.get("ts", 0))
        except (TypeError, ValueError):
            tick_ts = 0.0

        profit_per_dollar = self._cfg.profit_threshold_per_dollar
        loss_per_dollar = self._cfg.loss_threshold_per_dollar
        trail_arm_per_dollar = self._cfg.trail_arm_per_dollar
        warmup_s = self._cfg.min_evaluation_age_s
        # Snapshot to a list — _firing modifications during iteration
        # would mutate the set we're iterating in some Python versions.
        for pid in list(position_ids):
            if pid in self._firing:
                continue
            tracked = self._open.get(pid)
            if tracked is None:
                continue
            # 2026-05-05 warmup gate. Skip TP/SL_ABS evaluation when the
            # position is too fresh — its `entry_price` is the limit
            # we set, not the actual fill, so the first tick can show
            # ±50% drift unrelated to market movement. Skip ONLY when:
            #   - cfg has a positive warmup window
            #   - position carries a real entry_ts (>0)
            #   - elapsed in [0, warmup) — negative elapsed (clock skew
            #     or simulated test timestamps) is treated as "warmup
            #     not applicable" so the gate doesn't block forever
            #
            # 2026-05-07 Phase 13 (canary v19 regression). When tick_ts
            # is 0 the WS payload omitted the `ts` field — Polymarket
            # occasionally pushes initial-state snapshots that way. The
            # pre-Phase-13 predicate `tick_ts > 0` short-circuited the
            # gate and let the very first WS tick (often a stale pre-
            # fill orderbook print) fire EXIT_SL_ABS within the same
            # second as the BUY. Pos 22434 hit exactly this: $0.49 fill
            # → first WS tick $0.43 with no ts → SL fired (-12.24%) →
            # Phase 4 restate later confirmed real fill at $0.59. Fix
            # is to fall back to wall-clock for the elapsed
            # calculation when tick_ts is missing/0; the gate then
            # behaves as if the tick arrived "right now" relative to
            # entry_ts, which is the only sane default for a payload
            # that has no time information.
            #
            # Trailing-arm/trail-floor logic still updates peak_price
            # via fall-through; skipping decisions only.
            if warmup_s > 0 and tracked.entry_ts > 0:
                effective_tick_ts = tick_ts if tick_ts > 0 else time.time()
                elapsed = effective_tick_ts - tracked.entry_ts
                if 0 <= elapsed < warmup_s:
                    # Record the warmup-blocked tick so post-incident
                    # debugging can answer 'how many ticks did warmup
                    # suppress before the position closed?'
                    await self._record_eval(
                        tracked=tracked,
                        tick_ts=int(tick_ts) if tick_ts else None,
                        tick_price=tick_price,
                        decision=ExitDecision.HOLD,
                        block_reason="warmup",
                        threshold_usd=Decimal("0"),
                    )
                    continue
            unrealized = (tick_price - tracked.entry_price) * tracked.shares
            # 2026-05-08 PHASE 24.5 — TP dollar floor. Effective TP
            # threshold is max(per-dollar × cost, tp_floor_usd) so a
            # tiny position can't fire +5% TP at +$0.075 unrealized
            # when the round-trip drag is ~$0.20. Same floor applies
            # to the trail-arm threshold so trailing doesn't engage
            # before drag-coverage either.
            tp_floor_usd = self._cfg.tp_floor_usd
            profit_usd = max(
                profit_per_dollar * tracked.cost_basis_usd, tp_floor_usd,
            )
            trail_arm_usd = max(
                trail_arm_per_dollar * tracked.cost_basis_usd, tp_floor_usd,
            )
            loss_usd = -(loss_per_dollar * tracked.cost_basis_usd)

            reason: ExitDecision | None = None
            threshold_usd = Decimal("0")

            if tracked.trail_armed:
                # Update peak.
                if tick_price > tracked.peak_price:
                    tracked.peak_price = tick_price
                # Trailing floor: peak minus a giveback fraction of
                # the peak gain, but never below entry × (1 + lock_pct).
                peak_gain = tracked.peak_price - tracked.entry_price
                if peak_gain < 0:
                    peak_gain = Decimal("0")
                trail_floor = max(
                    tracked.peak_price - peak_gain * self._cfg.trail_giveback_pct,
                    tracked.entry_price
                    * (Decimal("1") + self._cfg.trail_lock_pct),
                )
                if tick_price <= trail_floor:
                    reason = ExitDecision.EXIT_TP_TRAIL
                    threshold_usd = (
                        (trail_floor - tracked.entry_price) * tracked.shares
                    )
            elif (
                trail_arm_per_dollar > 0
                and unrealized >= trail_arm_usd
            ):
                # Arm trailing instead of firing the fixed TP — let the
                # winner run and lock gains via the trail floor.
                tracked.trail_armed = True
                tracked.peak_price = tick_price
                self.stats["trail_armed"] += 1
                logger.info(
                    "profit_taker: armed trailing on position %s "
                    "(entry=%s, arm_tick=%s, lock_floor=$%.4f)",
                    pid, tracked.entry_price, tick_price,
                    float(tracked.entry_price
                          * (Decimal("1") + self._cfg.trail_lock_pct)),
                )
                continue
            elif profit_per_dollar > 0 and unrealized >= profit_usd:
                # Trailing disabled (trail_arm_per_dollar=0): fall back
                # to the fixed-threshold profit-take.
                reason = ExitDecision.EXIT_TP_ABS
                threshold_usd = profit_usd
            elif loss_per_dollar > 0 and unrealized <= loss_usd:
                reason = ExitDecision.EXIT_SL_ABS
                threshold_usd = loss_usd

            if reason is None:
                # Eval ran, no trigger — record as HOLD so we know the
                # tick was observed and how close to thresholds it was.
                await self._record_eval(
                    tracked=tracked,
                    tick_ts=int(tick_ts) if tick_ts else None,
                    tick_price=tick_price,
                    decision=ExitDecision.HOLD,
                    block_reason=None,
                    threshold_usd=Decimal("0"),
                )
                continue

            # Exit decision — record the trigger BEFORE publishing so
            # the row lands even if the bus publish raises.
            await self._record_eval(
                tracked=tracked,
                tick_ts=int(tick_ts) if tick_ts else None,
                tick_price=tick_price,
                decision=reason,
                block_reason=None,
                threshold_usd=threshold_usd,
            )

            self._firing.add(pid)
            self.stats["exits_fired"] += 1
            if reason is ExitDecision.EXIT_TP_TRAIL:
                self.stats["trail_exits"] += 1
            logger.info(
                "profit_taker: closing position %s reason=%s — "
                "unrealized $%.4f vs threshold $%.4f (cost=$%s, "
                "entry=%s, tick=%s, shares=%s, peak=%s)",
                pid, reason.value, unrealized, threshold_usd,
                tracked.cost_basis_usd, tracked.entry_price,
                tick_price, tracked.shares,
                tracked.peak_price if tracked.trail_armed else "n/a",
            )
            await self._bus.publish(
                EVT_SELL_INTENT,
                {
                    "position_id": pid,
                    "token_id": tracked.token_id,
                    "shares": float(tracked.shares),
                    "strategy": tracked.strategy,
                    "reason": reason.value,
                    "price_hint": float(tick_price),
                },
            )

    async def _record_eval(
        self,
        *,
        tracked: _Tracked,
        tick_ts: int | None,
        tick_price: Decimal,
        decision: ExitDecision,
        block_reason: str | None,
        threshold_usd: Decimal,
    ) -> None:
        """Write one exit_evals row for this tick eval. No-op when no
        recorder is wired. Errors are caught + logged so a DB write
        failure can never block the eval loop — observability is a
        side-channel."""
        if self._eval_recorder is None:
            return
        try:
            from poly_terminal.persistence.repositories.exit_evals import (
                SOURCE_PROFIT_TAKER,
            )
            entry = tracked.entry_price
            unrealized = (tick_price - entry) * tracked.shares
            pct_move = (
                (tick_price - entry) / entry if entry > 0 else Decimal("0")
            )
            details: dict[str, Any] = {
                "profit_threshold_per_dollar": float(
                    self._cfg.profit_threshold_per_dollar
                ),
                "loss_threshold_per_dollar": float(
                    self._cfg.loss_threshold_per_dollar
                ),
                "trail_arm_per_dollar": float(self._cfg.trail_arm_per_dollar),
                "trail_armed": tracked.trail_armed,
                "peak_price": float(tracked.peak_price),
                "threshold_usd": float(threshold_usd),
                "cost_basis_usd": float(tracked.cost_basis_usd),
                "shares": float(tracked.shares),
                "min_evaluation_age_s": int(self._cfg.min_evaluation_age_s),
            }
            await self._eval_recorder.record(
                position_id=tracked.position_id,
                token_id=tracked.token_id,
                strategy=tracked.strategy,
                eval_ts=int(time.time()),
                tick_ts=tick_ts,
                price_source=SOURCE_PROFIT_TAKER,
                price_used=float(tick_price),
                entry_price=float(entry),
                pct_move=float(pct_move),
                unrealized_usd=float(unrealized),
                decision=decision.value,
                block_reason=block_reason,
                details=details,
            )
        except Exception:
            logger.exception(
                "profit_taker: exit_evals record failed for pid=%s "
                "(non-fatal)",
                tracked.position_id,
            )
