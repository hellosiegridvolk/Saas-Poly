"""Exit Agent — orchestrates ExitDecisionEngine over the bus.

Subscribes to:
  EVT_POSITION_OPENED — begin watching the position
  EVT_MARKET_TICK     — evaluate decision on every price update
  EVT_WALLET_FILL     — top-rank wallet sell triggers EXIT_WHALE_OUT

Plus a periodic BarResolutionWatcher coroutine that closes positions
whose underlying bar has resolved (parsed from end_date_iso). Without
this, short-window positions that never receive another tick will sit
open until the strategy's max_hold_seconds expires — long after the
underlying market has settled and the position is dead at fill price.

Publishes:
  EVT_SELL_INTENT — when any branch decides to exit
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.exit.decision_engine import ExitDecisionEngine
from poly_terminal.agents.exit.position_state import PositionState
from poly_terminal.agents.strategy.exit_config import ExitConfig, for_strategy
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_MARKET_TICK,
    EVT_POSITION_OPENED,
    EVT_SELL_INTENT,
    EVT_WALLET_FILL,
)
from poly_terminal.persistence.repositories.exit_evals import (
    SOURCE_BAR_WATCHER,
    SOURCE_MARKET_WS,
    SOURCE_TICK_POLLER,
    SOURCE_WHALE_OUT,
    ExitEvalsRepo,
)
from poly_terminal.persistence.repositories.fills import PositionsRepo
from poly_terminal.shared.enums import ExitDecision

logger = logging.getLogger(__name__)


def _parse_end_date_iso(end_date_iso: str | None) -> int | None:
    if not end_date_iso:
        return None
    try:
        dt = datetime.fromisoformat(str(end_date_iso).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


class ExitAgent:
    """Owns per-position state + the decision engine.

    Single instance; keyed by position_id. Multiple positions on the same
    token are evaluated independently.
    """

    def __init__(
        self,
        bus: EventBus,
        engine: ExitDecisionEngine | None = None,
        bar_check_interval_s: float = 5.0,
        now_reader: Callable[[], int] | None = None,
        positions_repo: PositionsRepo | None = None,
        shadow_price_fn: Callable[[str, str], Any] | None = None,
        shadow_price_timeout_s: float = 2.0,
        eval_recorder: "ExitEvalsRepo | None" = None,
    ) -> None:
        self._bus = bus
        self._engine = engine or ExitDecisionEngine()
        # 2026-05-05: optional per-evaluation observability sink. When
        # supplied, every tick / wallet-signal / bar-watcher decision
        # writes one row to `exit_evals` so post-incident debugging can
        # answer "did warmup block this?" / "what price source was used
        # at the moment we'd have hit TP?". Optional so unit tests that
        # don't care about the trace continue to work unchanged.
        self._eval_recorder = eval_recorder
        self._positions: dict[int, PositionState] = {}
        self._configs: dict[int, ExitConfig] = {}
        self._strategy_by_pos: dict[int, str] = {}
        # token_id → set[position_id] for fast tick fan-out
        self._by_token: dict[str, set[int]] = {}
        self._bar_end_ts: dict[int, int] = {}  # position_id → end_ts (epoch s)
        self._followed_wallets: set[str] = set()
        self._bar_check_interval_s = bar_check_interval_s
        self._now = now_reader or (lambda: int(datetime.now(timezone.utc).timestamp()))
        self._positions_repo = positions_repo
        # 2026-05-04 shadow-execution patch: optional async callable
        # invoked when bar_watcher is about to close a position whose
        # last_price has never been updated by a market tick. Signature:
        # (token_id, side) → Awaitable[float | None] where side is the
        # POSITION side ("BUY" → query best_bid because we'd SELL to
        # close; "SELL" → query best_ask). Returns the shadow exit
        # price or None to fall back to entry_price.
        # Without this, bar_watcher fallback to entry_price produces
        # exactly $0 realized PnL on every tick-starved close — see
        # 2026-05-04 audit. With this, close_position records the
        # actual price we'd receive if we marketable-SELL right now.
        self._shadow_price_fn = shadow_price_fn
        # 2026-05-05: per-call timeout for the shadow_price_fn invocation.
        # The bot's get_best_bid → py-clob-client-v2 → requests path has
        # NO HTTP timeout by default. A single hung request blocks the
        # entire bar_watcher loop because we await sequentially on each
        # close. Production observation: 12:15-12:25 the bot stopped
        # closing entirely (cap held at 50/50 with 5 oldest positions
        # 41 min past their end_date_iso) — diagnosed as exactly this.
        # Bound each call so a single stuck token can't wedge bar_watcher.
        # Float, not int — tests inject sub-second values.
        self._shadow_price_timeout_s = float(shadow_price_timeout_s)
        self._stats = {
            "shadow_price_used": 0,
            "shadow_price_zero_settlement": 0,
            "shadow_price_fallback_to_entry": 0,
            "shadow_price_timeout": 0,
            "max_hold_exits": 0,
        }
        self._started = False

    @property
    def tracked_position_ids(self) -> set[int]:
        return set(self._positions.keys())

    def set_followed_wallets(self, wallets: set[str]) -> None:
        """Update the followed-wallet set (used for whale-out detection)."""
        self._followed_wallets = {w.lower() for w in wallets}

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_POSITION_OPENED, self._on_open)
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)
        self._bus.subscribe(EVT_WALLET_FILL, self._on_wallet_fill)
        if self._positions_repo is not None:
            await self._restore_open_positions()
        self._started = True

    async def _restore_open_positions(self) -> int:
        """Rebuild in-memory state from positions table after a restart.

        The in-memory dicts (`_positions`, `_configs`, `_bar_end_ts`,
        `_by_token`, `_strategy_by_pos`) are lost on every process
        restart. Without this, positions opened in a prior process are
        invisible to the bar watcher and will sit open forever — which
        leaks the MAX_OPEN_POSITIONS cap. Strategy is unknown
        post-restart (positions row only stores entry_intent_id), so we
        attach a default ExitConfig keyed off the empty-string strategy
        — bar resolution doesn't consult the config, and TP/SL paths
        for restored positions degrade to the default profile, which is
        acceptable for recovery semantics.

        Returns the number of positions restored.
        """
        repo = self._positions_repo
        if repo is None:
            return 0
        rows = await repo.fetch_all_open()
        for r in rows:
            pid = int(r["position_id"])
            if pid in self._positions:
                continue
            token_id = str(r["token_id"])
            end_ts = _parse_end_date_iso(r.get("end_date_iso"))
            try:
                pos = PositionState(
                    position_id=pid,
                    token_id=token_id,
                    entry_price=Decimal(str(r["entry_price"])),
                    shares=Decimal(str(r["shares"])),
                    cost_basis_usd=Decimal(str(r["cost_basis_usd"])),
                    entry_ts=float(r["entry_ts"]),
                    # Item #5: also restore bar_end_ts so the
                    # dynamic warmup cap is correct after restart.
                    bar_end_ts=float(end_ts) if end_ts is not None else None,
                )
            except (TypeError, ValueError):
                logger.exception("ExitAgent: malformed position row pid=%s", pid)
                continue
            self._positions[pid] = pos
            # 2026-05-05: pull the originating strategy via LEFT JOIN
            # in PositionsRepo.fetch_all_open. With strategy known, we
            # apply the right ExitConfig (and the right max_hold).
            # Without strategy (imported positions, or rows where the
            # join missed), strategy is "" — config falls back to
            # safe defaults and check_bar_resolutions skips the
            # max_hold branch.
            strategy = str(r.get("strategy") or "")
            self._configs[pid] = for_strategy(strategy)
            self._strategy_by_pos[pid] = strategy
            self._by_token.setdefault(token_id, set()).add(pid)
            if end_ts is not None:
                self._bar_end_ts[pid] = end_ts
        return len(rows)

    # ── Handlers ─────────────────────────────────────────────────────

    async def _on_open(self, event: str, payload: Any) -> None:
        try:
            position_id = int(payload["position_id"])
            token_id = str(payload["token_id"])
            strategy = str(payload.get("strategy", ""))
            entry_price = Decimal(str(payload["entry_price"]))
            shares = Decimal(str(payload["shares"]))
            cost_basis_usd = Decimal(str(payload["cost_basis_usd"]))
            entry_ts = float(payload["entry_ts"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("ExitAgent: malformed position_opened payload: %s", exc)
            return
        cfg = payload.get("exit_config") or for_strategy(strategy)
        # Bar-resolution watcher: capture end_date_iso so we can force-exit
        # short-window positions whose underlying bar has resolved.
        end_ts = _parse_end_date_iso(payload.get("end_date_iso"))
        pos = PositionState(
            position_id=position_id,
            token_id=token_id,
            entry_price=entry_price,
            shares=shares,
            cost_basis_usd=cost_basis_usd,
            entry_ts=entry_ts,
            # 2026-05-05 (deep-research-23 item #5): pass bar_end_ts
            # into the position state so the decision engine can cap
            # warmup proportional to time-remaining-at-entry.
            bar_end_ts=float(end_ts) if end_ts is not None else None,
        )
        self._positions[position_id] = pos
        self._configs[position_id] = cfg
        self._strategy_by_pos[position_id] = strategy
        self._by_token.setdefault(token_id, set()).add(position_id)
        if end_ts is not None:
            self._bar_end_ts[position_id] = end_ts

    async def _on_tick(self, event: str, payload: Any) -> None:
        try:
            token_id = str(payload["token_id"])
            price = Decimal(str(payload["price"]))
            ts = float(payload.get("ts", 0))
        except (KeyError, TypeError, ValueError):
            return
        # 2026-05-07 Phase 13 (canary v19 regression). When the WS
        # payload omits the `ts` field — Polymarket sometimes pushes
        # initial-state snapshots that way — substitute wall-clock
        # (via the injected now_reader) so the decision_engine's
        # warmup gate sees a sensible elapsed number instead of
        # `now_ts=0 - entry_ts ≈ -1.78e9` which made the
        # `0 <= elapsed < warmup_s` predicate false and let the very
        # first WS tick fire SL immediately. Pos 22434 hit this:
        # $0.49 fill → first market_ws tick {price: 0.43} with no ts
        # → SL fired (-12.24%) → Phase 4 restate later confirmed real
        # fill at $0.59. The substitution preserves existing behavior
        # for ticks that DO carry a ts (including synthetic-replay
        # tests that use deliberately old timestamps — those still
        # produce a negative elapsed and skip warmup). Using
        # `self._now()` keeps the test seam in place: tests with an
        # injected `now_reader` get a deterministic clock for the
        # max_hold / bar-resolution paths that share this same now.
        if ts <= 0:
            ts = float(self._now())
        # MarketDispatcher / TickPoller stamp `side="POLL"` on synthetic
        # REST-poll ticks so we can attribute the price source. WS ticks
        # use 'BUY' / 'SELL' / no key. Map to canonical source label.
        side_field = str(payload.get("side", "")).upper()
        price_source = (
            SOURCE_TICK_POLLER if side_field == "POLL" else SOURCE_MARKET_WS
        )
        position_ids = list(self._by_token.get(token_id, set()))
        for pid in position_ids:
            pos = self._positions.get(pid)
            cfg = self._configs.get(pid)
            if pos is None or cfg is None:
                continue
            result = self._engine.evaluate_with_reason(
                pos, price, cfg, now_ts=ts
            )
            await self._record_eval(
                pos=pos,
                cfg=cfg,
                tick_ts=int(ts) if ts else None,
                price_source=price_source,
                price_used=float(price),
                result=result,
                source_extra={"side_field": side_field},
            )
            if result.decision is ExitDecision.HOLD:
                continue
            await self._emit_sell(pid, result.decision, price)

    async def _on_wallet_fill(self, event: str, payload: Any) -> None:
        wallet = str(payload.get("wallet", "")).lower()
        if wallet not in self._followed_wallets:
            return
        side = str(payload.get("side", "")).upper()
        if side != "SELL":
            return
        token_id = str(payload.get("token_id", ""))
        position_ids = list(self._by_token.get(token_id, set()))
        for pid in position_ids:
            pos = self._positions.get(pid)
            cfg = self._configs.get(pid)
            if pos is None or cfg is None:
                continue
            eval_price = pos.last_price if pos.last_price > 0 else pos.entry_price
            tick_ts = float(payload.get("ts", pos.entry_ts))
            # Whale-out short-circuits the rest of the engine.
            result = self._engine.evaluate_with_reason(
                pos,
                eval_price,
                cfg,
                now_ts=tick_ts,
                whale_out=True,
            )
            await self._record_eval(
                pos=pos,
                cfg=cfg,
                tick_ts=int(tick_ts) if tick_ts else None,
                price_source=SOURCE_WHALE_OUT,
                price_used=float(eval_price),
                result=result,
                source_extra={"signal_wallet": wallet},
            )
            await self._emit_sell(pid, result.decision, pos.last_price or pos.entry_price)

    async def _emit_sell(
        self, position_id: int, decision: ExitDecision, price: Decimal
    ) -> None:
        pos = self._positions.pop(position_id, None)
        cfg = self._configs.pop(position_id, None)
        strategy = self._strategy_by_pos.pop(position_id, "")
        self._bar_end_ts.pop(position_id, None)
        if pos is None or cfg is None:
            return
        token_set = self._by_token.get(pos.token_id)
        if token_set is not None:
            token_set.discard(position_id)
            if not token_set:
                self._by_token.pop(pos.token_id, None)
        await self._bus.publish(
            EVT_SELL_INTENT,
            {
                "position_id": position_id,
                "token_id": pos.token_id,
                "shares": pos.shares,
                "strategy": strategy,
                "reason": decision.value,
                "price_hint": price,
            },
        )

    # ── Bar-resolution watcher ────────────────────────────────────────

    async def check_bar_resolutions(self) -> int:
        """Force EXIT_TIME on any tracked position whose underlying bar has
        resolved (now ≥ bar_end_ts) OR whose strategy max_hold_seconds
        has expired. Returns count of positions exited.

        Two branches:
          1. bar_end (end_date_iso passed) — for short-window markets
             that resolve at a fixed wall-clock time.
          2. max_hold (entry_ts + max_hold_seconds passed) — for tokens
             that don't receive EVT_MARKET_TICK events. Without a tick,
             the time-stop in the decision engine never fires, leaving
             positions open until the bar resolves (potentially hours
             after max_hold). 2026-05-05 audit found 26/240 (11%)
             copy_scalp closes were over the 10-min max_hold for this
             exact reason; six positions were 17-51 min old at audit
             time, sitting idle because no ticks had arrived for those
             tokens.

        2026-05-04 shadow-execution patch: when last_price was never
        set by a market tick (illiquid token + sparse WS book updates),
        query the live orderbook for the best contra price instead of
        falling back to entry_price. This converts "every flat close
        is exactly $0" into "every flat close uses the realistic exit
        price we'd actually get from a marketable SELL right now".
        """
        now = self._now()
        to_exit: set[int] = set()
        # Branch 1: bar end_date_iso passed.
        for pid, end_ts in self._bar_end_ts.items():
            if now >= end_ts:
                to_exit.add(pid)
        # Branch 2: strategy max_hold_seconds expired. Iterate
        # _positions (not _bar_end_ts) — positions without an
        # end_date_iso still need this guard.
        max_hold_pids: set[int] = set()
        for pid, pos in self._positions.items():
            if pid in to_exit:
                continue
            cfg = self._configs.get(pid)
            if cfg is None:
                continue
            # Skip max_hold for restored positions (strategy unknown).
            # The default ExitConfig.max_hold_seconds is 300s — far
            # too aggressive for a position whose true strategy might
            # be copy_trade (24h max_hold). bar_end (when present)
            # still closes restored positions on schedule.
            if not self._strategy_by_pos.get(pid):
                continue
            if (now - pos.entry_ts) >= cfg.max_hold_seconds:
                to_exit.add(pid)
                max_hold_pids.add(pid)
        for pid in to_exit:
            pos = self._positions.get(pid)
            if pos is None:
                self._bar_end_ts.pop(pid, None)
                continue
            cfg = self._configs.get(pid)
            price = await self._resolve_exit_price(pos)
            # Record the bar-watcher trip BEFORE emit_sell pops state —
            # otherwise pos/cfg are unavailable by the time we'd record.
            if self._eval_recorder is not None and cfg is not None:
                from poly_terminal.agents.exit.decision_engine import (
                    ExitEvalResult,
                )
                pct = pos.pct_move(price) if price > 0 else Decimal("0")
                un = pos.unrealized_usd(price) if price > 0 else Decimal("0")
                result = ExitEvalResult(
                    decision=ExitDecision.EXIT_TIME,
                    block_reason=None,
                    pct_move=pct,
                    unrealized_usd=un,
                )
                await self._record_eval(
                    pos=pos,
                    cfg=cfg,
                    tick_ts=int(now),
                    price_source=SOURCE_BAR_WATCHER,
                    price_used=float(price),
                    result=result,
                    source_extra={
                        "branch": "max_hold" if pid in max_hold_pids else "bar_end",
                    },
                )
            await self._emit_sell(pid, ExitDecision.EXIT_TIME, price)
            if pid in max_hold_pids:
                self._stats["max_hold_exits"] += 1
        return len(to_exit)

    async def _record_eval(
        self,
        *,
        pos: PositionState,
        cfg: ExitConfig,
        tick_ts: int | None,
        price_source: str,
        price_used: float,
        result: Any,  # ExitEvalResult — typed loosely to avoid circular imports
        source_extra: dict[str, Any] | None = None,
    ) -> None:
        """Write one exit_evals row. No-op when no recorder is wired.

        Errors are caught + logged so a DB write failure can never block
        the exit decision loop. The recorder is observability — the
        primary control flow must always survive its absence."""
        if self._eval_recorder is None:
            return
        try:
            details: dict[str, Any] = {
                "tp_pct": float(cfg.tp_pct),
                "sl_pct": float(cfg.sl_pct),
                "sl_floor_usd": float(cfg.sl_floor_usd),
                "max_hold_seconds": int(cfg.max_hold_seconds),
                "min_evaluation_age_s": int(cfg.min_evaluation_age_s),
                "adverse_tick_count": int(pos.adverse_tick_count),
                "shares": float(pos.shares),
            }
            if source_extra:
                details.update(source_extra)
            eval_ts = self._now()
            await self._eval_recorder.record(
                position_id=pos.position_id,
                token_id=pos.token_id,
                strategy=self._strategy_by_pos.get(pos.position_id, ""),
                eval_ts=eval_ts,
                tick_ts=tick_ts,
                price_source=price_source,
                price_used=price_used,
                entry_price=float(pos.entry_price),
                pct_move=float(result.pct_move),
                unrealized_usd=float(result.unrealized_usd),
                decision=result.decision.value,
                block_reason=result.block_reason,
                details=details,
            )
        except Exception:
            logger.exception(
                "ExitAgent: exit_evals record failed for pid=%s (non-fatal)",
                pos.position_id,
            )
            return
        # 2026-05-09 PHASE 30(b) — broadcast eval-recorded event so
        # FreshnessTracker can update last_eval_ts. Without this, the
        # `live_canary_ready` rollup at /api/freshness has no upstream
        # signal — every active position flips eval_stale=True after
        # 60s of operation, even though the bot is happily evaluating
        # ticks. v48 verified the endpoint, this fix verifies the feed.
        try:
            await self._bus.publish(
                "exit.eval.recorded",
                {
                    "position_id": pos.position_id,
                    "token_id": pos.token_id,
                    "eval_ts": eval_ts,
                    "decision": result.decision.value,
                    "price_source": price_source,
                },
            )
        except Exception:
            logger.exception(
                "ExitAgent: exit.eval.recorded publish failed for pid=%s "
                "(non-fatal)",
                pos.position_id,
            )

    async def _resolve_exit_price(self, pos: PositionState) -> Decimal:
        """Resolve the price to mark a position closed at.

        Order of preference:
          1. pos.last_price if a tick has updated it.
          2. Live orderbook best contra (shadow-execution patch).
             • shadow > 0   → real exit price (good close).
             • shadow == 0  → contra book is empty. On Polymarket that
                              means the market resolved AGAINST this
                              side; the position is worth $0. Record
                              the truthful settlement instead of
                              falling back to entry_price (which
                              silently records realized PnL=$0 and
                              hides the loss — see 2026-05-05 audit:
                              277/455 closes (61%) were hidden losses
                              for this exact reason).
             • shadow is None → fetch failed; fall back to entry_price.
          3. pos.entry_price as last-resort fallback.
        """
        if pos.last_price > 0:
            return pos.last_price
        # Try the shadow-price fallback if wired.
        if self._shadow_price_fn is not None and pos.token_id:
            # PositionState doesn't carry side (every position the bot
            # opens is a BUY in v3). Closing a BUY = SELL on the wire,
            # which fills against the best BID. Pass "BUY" so the
            # shadow_price_fn knows the position is long and queries
            # the bid side.
            try:
                shadow = await asyncio.wait_for(
                    self._shadow_price_fn(pos.token_id, "BUY"),
                    timeout=self._shadow_price_timeout_s,
                )
            except asyncio.TimeoutError:
                # 2026-05-05 patch: bound the wait so one hung HTTP
                # call can't wedge bar_watcher across all pending
                # closes. Treat timeout the same as None (fetch
                # failed) → fall through to entry_price fallback.
                logger.warning(
                    "exit_agent: shadow_price_fn timed out (%.1fs) "
                    "for token %s — falling back to entry_price",
                    self._shadow_price_timeout_s, pos.token_id,
                )
                self._stats["shadow_price_timeout"] += 1
                shadow = None
            except Exception:
                logger.exception(
                    "exit_agent: shadow_price_fn raised for token %s",
                    pos.token_id,
                )
                shadow = None
            if shadow is not None:
                if shadow > 0:
                    self._stats["shadow_price_used"] += 1
                else:
                    self._stats["shadow_price_zero_settlement"] += 1
                return Decimal(str(shadow))
        # Final fallback: entry_price (produces $0 realized PnL — only
        # reached when shadow_price_fn raised or returned None).
        self._stats["shadow_price_fallback_to_entry"] += 1
        return pos.entry_price

    async def run_bar_watcher(self, shutdown: asyncio.Event) -> None:
        """Periodic loop. Spawn alongside other run_tasks in PolyTerminal."""
        while not shutdown.is_set():
            try:
                await self.check_bar_resolutions()
            except Exception:
                logger.exception("bar_watcher: iteration failed")
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self._bar_check_interval_s
                )
                return
            except asyncio.TimeoutError:
                continue
