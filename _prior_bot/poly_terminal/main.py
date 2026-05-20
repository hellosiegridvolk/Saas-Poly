"""Composition root for Poly Terminal Final v3.

Boot sequence (paper-mode default; safety locks enforced before any agent
construction):

  1. Apply PARAMS_PRESET overlay BEFORE pydantic-settings reads .env.
  2. Run preflight (fail-fast on RC drift) — exits 2 on drift.
  3. Construct Settings; verify mode_lock + paper_mode + armed invariants.
  4. Open SQLite, apply migrations.
  5. Build per-agent latency budgets.
  6. Build agents in dependency order:
       data clients → wallet_intel → orderbook_intel → context →
       strategy → risk → execution → exit → monitor.
  7. Start each agent (subscribe to bus).
  8. Run the FastAPI monitor on its own task.
  9. Wait for SIGINT/SIGTERM; clean shutdown.

Exit codes:
  0  clean shutdown
  1  startup error other than config drift
  2  preflight rejected config drift
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_terminal.agents.strategy.lane_book import LaneBook, LaneRealizedCache

import uvicorn

from poly_terminal.agents.auto_tuner.agent import (
    AutoTunerAgent,
    AutoTunerConfig,
)
from poly_terminal.agents.cash_guard.agent import (
    CashGuardAgent,
    CashGuardConfig,
)
from poly_terminal.agents.context.agent import ContextAgent, ContextConfig
from poly_terminal.agents.context.discovery import (
    DiscoveryAgent,
    DiscoveryConfig,
)
from poly_terminal.agents.execution.agent import ExecutionAgent
from poly_terminal.agents.execution.live_fill_reconciler import (
    LiveFillReconciler,
)
from poly_terminal.agents.exit.agent import ExitAgent
from poly_terminal.agents.freshness.agent import FreshnessTracker
from poly_terminal.agents.held_token_subscriber.agent import (
    HeldTokenSubscriberAgent,
)
from poly_terminal.agents.inventory_reconciler.agent import (
    InventoryReconcilerAgent,
    InventoryReconcilerConfig,
)
from poly_terminal.agents.inventory_reconciler.ctf_reader import (
    reader_from_settings as ctf_reader_from_settings,
)
from poly_terminal.agents.monitor.agent import build_app
from poly_terminal.agents.monitor.state import MonitorState
from poly_terminal.agents.orderbook_intel.agent import OrderbookIntelAgent
from poly_terminal.agents.orderbook_intel.imbalance import ImbalanceConfig
from poly_terminal.agents.orders_recorder.agent import (
    OrdersRecorderAgent,
)
from poly_terminal.agents.position_importer.agent import (
    PositionImporterAgent,
    PositionImporterConfig,
)
from poly_terminal.agents.profit_taker.agent import (
    ProfitTakerAgent,
    ProfitTakerConfig,
)
from poly_terminal.agents.redeemer.agent import (
    GammaMarketResolver,
    RedeemerAgent,
    RedeemerConfig,
)
from poly_terminal.agents.risk.agent import RiskAgent
from poly_terminal.agents.risk.gates.buy_cooldown import BuyCooldownGate
from poly_terminal.agents.risk.gates.daily_loss import DailyLossGate
from poly_terminal.agents.risk.gates.duplicate_intent import DuplicateIntentGate
from poly_terminal.agents.risk.gates.entry_liquidity import (
    EntryLiquidityConfig,
    EntryLiquidityGate,
)
from poly_terminal.agents.risk.gates.heartbeat_alive import HeartbeatAliveGate
from poly_terminal.agents.risk.gates.latency_budget import LatencyBudgetGate
from poly_terminal.agents.risk.gates.market_concentration import (
    MarketConcentrationGate,
)
from poly_terminal.agents.risk.gates.open_positions import OpenPositionsGate
from poly_terminal.agents.risk.gates.per_trade_size import PerTradeSizeGate
from poly_terminal.agents.risk.gates.time_left import TimeLeftGate
from poly_terminal.agents.risk.gates.total_exposure import TotalExposureGate
from poly_terminal.agents.risk.metrics_middleware import metrics_middleware
from poly_terminal.agents.risk.mode_lock import ModeLockGate
from poly_terminal.agents.risk.pipeline import GatePipeline
from poly_terminal.agents.risk.reservation_ledger import (
    OpenPositionsReservationLedger,
)
from poly_terminal.agents.session_guard.agent import (
    SessionGuardAgent,
    SessionGuardConfig,
)
from poly_terminal.agents.strategy.allocator import (
    AllocatorConfig,
    LedgerSnapshot,
    RiskAllocator,
)
from poly_terminal.agents.strategy.copy_trade import (
    CopyTradeConfig,
    CopyTradeStrategy,
)
from poly_terminal.agents.strategy.dump_hedge import (
    DumpHedgeConfig,
    DumpHedgeStrategy,
)
from poly_terminal.agents.strategy.flash_crash import (
    FlashCrashConfig,
    FlashCrashStrategy,
)
from poly_terminal.agents.strategy.ledger_refresher import (
    LedgerSnapshotRefresher,
)
from poly_terminal.agents.strategy.scalp_window import (
    ScalpConfig,
    ScalpWindowStrategy,
)
from poly_terminal.agents.wallet_intel.activity_poller import (
    PollerConfig,
    WalletActivityPoller,
)
from poly_terminal.agents.wallet_intel.agent import WalletIntelAgent
from poly_terminal.agents.wallet_intel.ranker import RankerConfig
from poly_terminal.bus.event_bus import bus as default_bus
from poly_terminal.bus.events import (
    EVT_AGENT_HEARTBEAT,
    EVT_WALLET_FILL,
    EVT_WALLET_RANK_CHANGED,
    EVT_WATCHLIST_UPDATED,
)
from poly_terminal.data.polygon.log_subscriber import PolygonLogSubscriber
from poly_terminal.data.polygon.price_surge_detector import PriceSurgeDetector
from poly_terminal.config.fingerprint import compute_fingerprint
from poly_terminal.config.preset_loader import apply_preset_to_env
from poly_terminal.config.settings import Settings
from poly_terminal.data.clob.auth import (
    DerivedCreds,
    derive_l2_creds_from_private_key,
)
from poly_terminal.data.clob.live_orders import LiveOrderClient
from poly_terminal.data.data_api.client import DataApiClient
from poly_terminal.data.gamma.client import GammaClient
from poly_terminal.data.latency_budget import LatencyBudget
from poly_terminal.data.websocket.market import MarketWebSocket
from poly_terminal.data.websocket.user import UserWebSocket
from poly_terminal.persistence.db import Database
from poly_terminal.persistence.repositories.exit_evals import ExitEvalsRepo
from poly_terminal.persistence.repositories.fills import (
    FillsRepo,
    PositionsRepo,
)
from poly_terminal.persistence.repositories.gate_metrics import GateMetricsRepo
from poly_terminal.persistence.repositories.live_orders import LiveOrdersRepo
from poly_terminal.persistence.repositories.orders import OrdersRepo
from poly_terminal.persistence.repositories.wallets import WalletsRepo
from poly_terminal.scripts.preflight import main as preflight_main
from poly_terminal.shared.enums import BotMode

logger = logging.getLogger("poly_terminal.main")


def build_live_allowed_strategies(settings: Settings) -> frozenset[str]:
    """Map enabled-strategy flags to canonical `strategy_name`s.

    Phase 41.5 (2026-05-13) — extracted from `PolyTerminal.construct_agents`
    so the mapping can be unit-tested in isolation. The bug that
    motivated the extraction: the inline version of this mapping only
    listed `copy_trade`, `copy_scalp`, `endgame_yield` and silently
    dropped every other strategy from the LIVE allow-list. PAPER mode
    short-circuits the allow-list, so this was invisible until the
    first LIVE canary attempt produced 0 fills despite scalp_window
    publishing valid intents.

    Keep in sync with each strategy module's `name = "..."` declaration
    under `src/poly_terminal/agents/strategy/`.

    Args:
        settings: Application settings with `strategy_*` flags.

    Returns:
        A frozenset of canonical strategy names that should be allowed
        through the allocator's LIVE-mode gate.
    """
    allowed: set[str] = set()
    if settings.strategy_copy_trade:
        allowed.add("copy_trade")
    if settings.strategy_copy_scalp:
        allowed.add("copy_scalp")
    if settings.strategy_copy_scalp_active:
        allowed.add("copy_scalp_active")
    if settings.strategy_endgame_yield:
        allowed.add("endgame_yield")
    # scalp_window's two cadence flags (15m / 1h) both publish under
    # the same canonical strategy_name. Enabling either is sufficient.
    if settings.strategy_scalp_15m or settings.strategy_scalp_1h:
        allowed.add("scalp_window")
    if settings.strategy_certainty_farm:
        allowed.add("certainty_farm")
    if settings.strategy_flash_crash:
        allowed.add("flash_crash")
    if settings.strategy_dump_hedge:
        allowed.add("dump_hedge")
    if settings.strategy_crypto_bar_momentum:
        allowed.add("crypto_bar_momentum")
    return frozenset(allowed)


def build_bakeoff_strategies(
    lanes: list,
    *,
    bus: object,
    lane_book: object,
    mode_getter: object,
    ledger_snapshot_getter: object,
    copy_scalp_wallets: frozenset[str] | set[str] = frozenset(),
) -> list:
    """One strategy instance per ENABLED lane: .name overridden to the
    lane id, lane.per_trade_cap_usd threaded into the strategy's own
    config, shared LaneBook injected as allocator. Mirrors
    build_live_allowed_strategies (module-level, unit-tested). Strategy
    classes not mapped here are skipped with a warning (YAGNI)."""
    import logging as _logging
    from decimal import Decimal as _D

    from poly_terminal.agents.strategy.copy_scalp import (
        CopyScalpConfig,
        CopyScalpStrategy,
    )
    from poly_terminal.agents.strategy.copy_trade import (
        CopyTradeConfig,
        CopyTradeStrategy,
    )
    from poly_terminal.agents.strategy.dump_hedge import (
        DumpHedgeConfig,
        DumpHedgeStrategy,
    )
    from poly_terminal.agents.strategy.scalp_window import (
        ScalpConfig,
        ScalpWindowStrategy,
    )

    _log = _logging.getLogger(__name__)
    out: list = []
    for lane in lanes:
        if not lane.enabled:
            continue
        cap = _D(str(lane.per_trade_cap_usd))
        kw = dict(
            allocator=lane_book,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        if lane.strategy == "copy_trade":
            inst = CopyTradeStrategy(
                bus=bus,
                cfg=CopyTradeConfig(
                    max_position_usd=cap, max_position_usd_hard=cap,
                ),
                **kw,
            )
        elif lane.strategy == "copy_scalp":
            inst = CopyScalpStrategy(
                bus=bus,
                followed_wallets=copy_scalp_wallets,
                cfg=CopyScalpConfig(
                    max_position_usd=cap, max_position_usd_hard=cap,
                ),
                **kw,
            )
        elif lane.strategy in ("scalp_15m", "scalp_1h"):
            window = "15m" if lane.strategy == "scalp_15m" else "1h"
            inst = ScalpWindowStrategy(
                bus=bus,
                cfg=ScalpConfig(
                    window=window,
                    size_usd=cap,
                    bleed_band_lo=float(lane.params.get("bleed_band_lo", 0.0)),
                    bleed_band_hi=float(lane.params.get("bleed_band_hi", 0.0)),
                    entry_price_lo=float(lane.params.get("entry_price_lo", 0.0)),
                    entry_price_hi=float(lane.params.get("entry_price_hi", 0.0)),
                    min_seconds_to_resolution=int(
                        lane.params.get("min_seconds_to_resolution", 0)
                    ),
                ),
                **kw,
            )
        elif lane.strategy == "dump_hedge":
            _dh_def = DumpHedgeConfig()
            inst = DumpHedgeStrategy(
                bus=bus,
                cfg=DumpHedgeConfig(
                    size_usd=cap,
                    dump_pct=_D(str(lane.params.get("dump_pct", _dh_def.dump_pct))),
                    target_edge_pct=_D(str(lane.params.get("target_edge_pct", _dh_def.target_edge_pct))),
                    lookback_s=int(lane.params.get("lookback_s", _dh_def.lookback_s)),
                    cooldown_s=int(lane.params.get("cooldown_s", _dh_def.cooldown_s)),
                ),
                **kw,
            )
        else:
            _log.warning(
                "bakeoff: lane %s strategy %r not yet wired; skipping",
                lane.id, lane.strategy,
            )
            continue
        inst.name = lane.id
        out.append(inst)
    return out


def _python_version_guard() -> None:
    if sys.version_info < (3, 11) or sys.version_info >= (3, 13):
        print(
            f"[STARTUP] Python {sys.version_info.major}.{sys.version_info.minor} "
            "detected. Requires 3.11 or 3.12.",
            file=sys.stderr,
        )
        sys.exit(1)


def _configure_logging(log_level: str, log_format: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    if log_format == "json":
        # Minimal JSON-ish single-line formatter; structlog-compatible upgrade
        # path is in observability/logging.py for a future phase.
        fmt = '{"level":"%(levelname)s","name":"%(name)s","msg":"%(message)s","ts":%(created).0f}'
    else:
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    logging.basicConfig(level=level, format=fmt, force=True)


class PolyTerminal:
    """Composed system. Holds every agent + repo + lifecycle hooks."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bus = default_bus
        self.db = Database(settings.db_path)
        self.fills_repo: FillsRepo | None = None
        self.positions_repo: PositionsRepo | None = None
        # Live-mode plumbing — None in PAPER, populated in
        # construct_live_clients when a private key + funder are set.
        self.live_orders_repo: "LiveOrdersRepo | None" = None
        self.live_client: "LiveOrderClient | None" = None
        # Derived L2 credentials — set once at boot and reused by
        # UserWebSocket (auth headers) and LiveOrderClient (post_order).
        self._derived_l2_creds: "DerivedCreds | None" = None
        self.gate_metrics_repo: GateMetricsRepo | None = None
        self.wallets_repo: WalletsRepo | None = None
        # Phase 34 (2026-05-11) — closes the no-op allocator-gate hole.
        # Strategies now read this refresher's cached LedgerSnapshot
        # instead of constructing `LedgerSnapshot()` inline (which
        # always returned empty → position/exposure/one-strategy/
        # daily-loss gates all fired open).
        self.ledger_refresher: "LedgerSnapshotRefresher | None" = None
        self.lane_realized_cache: LaneRealizedCache | None = None
        self.lane_book: LaneBook | None = None
        # 2026-05-05 (deep-research-23 item #1): per-evaluation exit
        # observability sink. Wired into ExitAgent + ProfitTakerAgent
        # so every tick / poll / bar-watcher / whale-out decision lands
        # one row in `exit_evals`.
        self.exit_evals_repo: ExitEvalsRepo | None = None
        # Latency budgets (kept on the instance so /metrics can read them).
        self.gamma_budget = LatencyBudget(
            name="gamma",
            ceiling_ms=settings.latency_budget_gamma_ms,
            window_size=50,
            bus=self.bus,
        )
        self.clob_budget = LatencyBudget(
            name="clob_book",
            ceiling_ms=settings.latency_budget_clob_book_ms,
            window_size=50,
            bus=self.bus,
        )
        self.data_api_budget = LatencyBudget(
            name="data_api",
            ceiling_ms=2000,
            window_size=50,
            bus=self.bus,
        )
        # Agents.
        self.exit_agent: ExitAgent | None = None
        self.execution_agent: ExecutionAgent | None = None
        self.live_fill_reconciler: LiveFillReconciler | None = None
        self.risk_agent: RiskAgent | None = None
        self.orderbook_agent: OrderbookIntelAgent | None = None
        self.context_agent: ContextAgent | None = None
        self.wallet_agent: WalletIntelAgent | None = None
        self.copy_trade: CopyTradeStrategy | None = None
        self.flash_crash: FlashCrashStrategy | None = None
        self.scalp_15m: ScalpWindowStrategy | None = None
        self.scalp_1h: ScalpWindowStrategy | None = None
        self.dump_hedge: DumpHedgeStrategy | None = None
        # WebSockets.
        self.market_ws: MarketWebSocket | None = None
        self.user_ws: UserWebSocket | None = None
        # Discovery.
        self.gamma_client: GammaClient | None = None
        self.discovery: DiscoveryAgent | None = None
        # Activity poller — bridges Data API /activity → EVT_WALLET_FILL.
        self.data_api_client: DataApiClient | None = None
        self.activity_poller: WalletActivityPoller | None = None
        # Latency Options A+B: on-chain log subscriber + CLOB price-surge detector.
        self.polygon_log_sub: PolygonLogSubscriber | None = None
        self.price_surge_detector: PriceSurgeDetector | None = None
        # 2026-05-05 TickPoller — REST fallback that synthesizes
        # EVT_MARKET_TICK from /price polls when the WS feed is silent.
        # Constructed in _wire_run_tasks if TICK_POLLER_ENABLED=true.
        # Late-bound import inside the conditional to keep startup
        # cheap when disabled.
        from typing import Any as _Any  # avoid circular type ref
        self.tick_poller: _Any = None
        # Redeemer — periodic sweep of closed positions for resolved
        # markets. Auto-clears WORTHLESS, surfaces REDEEMABLE for
        # operator. No on-chain tx until relayer/web3 path lands.
        self.redeemer_agent: RedeemerAgent | None = None
        self.redeemer_resolver: GammaMarketResolver | None = None
        # ProfitTaker — absolute-threshold exit (+10¢ per $1 cost
        # basis by default). Independent of ExitDecisionEngine; fires
        # on first qualifying tick.
        self.profit_taker: ProfitTakerAgent | None = None
        # SessionGuard — halts trading at session-wide ±$20 PnL by
        # writing the kill-switch flag.
        self.session_guard: SessionGuardAgent | None = None
        # PositionImporter — discovers on-chain positions opened
        # outside the bot (manual UI trades, prior runs, etc.) and
        # registers them so ProfitTakerAgent + ExitDecisionEngine
        # start managing exits.
        self.position_importer: PositionImporterAgent | None = None
        # 2026-05-08 PHASE 30(a) — OrdersRecorderAgent.
        # Persists user-channel order lifecycle events (LIVE → MATCHED
        # → CANCELLED / EXPIRED) to the `orders` table so post-mortem
        # audits can distinguish "no SELL chosen" from "SELL chosen
        # but failed downstream." Closes the audit-trail gap flagged
        # in deep-research-report (26)/(27).
        self.orders_recorder: "OrdersRecorderAgent | None" = None  # noqa: F821
        # 2026-05-08 PHASE 30(b) — FreshnessTracker.
        # Per-position last_tick_age_ms / last_exit_eval_age_ms +
        # rollup `live_canary_ready`. Flips false the moment any
        # held position becomes tick-blind. Application-level
        # readiness signal that container/process probes cannot
        # express. Recommended by deep-research-report (26)/(27).
        self.freshness_tracker: "FreshnessTracker | None" = None  # noqa: F821
        # 2026-05-07 PHASE 10 — HeldTokenSubscriberAgent.
        # Auto-subscribes the market WebSocket to any token the bot
        # opens a position on. Without this, positions on tokens
        # NOT in recorder_tokens.txt's pre-subscribed set get ZERO
        # ticks → ProfitTaker can't fire TP/SL → bot rides every
        # position to bar resolution. Pos 22352 (May 7 canary v12)
        # was the smoking gun: +76.6% peak, 0 ticks, only the bar
        # resolution close fired, $2.35 loss instead of locked TP.
        self.held_token_subscriber: HeldTokenSubscriberAgent | None = None
        # Item #2: DB ↔ on-chain CTF balanceOf reconciler. Runs once
        # at boot in LIVE/LIVE_DRY/CLOSE_ONLY; fail-fast on drift.
        self.inventory_reconciler: InventoryReconcilerAgent | None = None
        self.inventory_report = None  # ReconciliationReport | None
        # AutoTuner — every 15 min, reads rolling realized PnL and
        # adjusts ProfitTaker thresholds to push back to positive.
        self.auto_tuner: AutoTunerAgent | None = None
        # CashGuard — pauses new BUYs (SELL exits unaffected) when
        # cumulative session PnL crosses a threshold; cooldown until
        # the top of the next hour.
        self.cash_guard: CashGuardAgent | None = None
        # 2026-05-05 CanaryController — auto-flips bot mode from LIVE
        # → CLOSE_ONLY after the first real LIVE fill. Bounds canary
        # blast radius to exactly one position. Constructed only when
        # bot_mode == LIVE at boot.
        from typing import Any as _Any
        self.canary_controller: _Any = None
        # Runtime mode override (None = use settings.bot_mode). The
        # canary controller flips this to CLOSE_ONLY on first LIVE
        # fill; the mode_getter wrapper picks it up immediately.
        self._mode_override: BotMode | None = None
        # Monitor.
        self.monitor_state: MonitorState | None = None
        self.shutdown = asyncio.Event()
        self._strategies: list = []

    async def initialize(self) -> None:
        await self.db.initialize()
        ok = await self.db.integrity_check()
        if ok != "ok":
            raise RuntimeError(f"DB integrity_check returned {ok!r}")
        self.fills_repo = FillsRepo(self.db)
        self.positions_repo = PositionsRepo(self.db)
        # Phase 34 — construct the refresher here so it's available to
        # strategy wiring. start() (which spawns the background loop +
        # primes the cache) is called in start_agents() once the event
        # loop is running.
        self.ledger_refresher = LedgerSnapshotRefresher(
            positions_repo=self.positions_repo,
        )
        self.gate_metrics_repo = GateMetricsRepo(self.db)
        self.wallets_repo = WalletsRepo(self.db)
        self.live_orders_repo = LiveOrdersRepo(self.db)
        self.exit_evals_repo = ExitEvalsRepo(self.db)
        # LiveOrderClient is built lazily in construct_live_clients so
        # PAPER mode without a private key still boots cleanly.
        self._construct_live_client_if_possible()

    def _mode_getter(self):
        """Returns a callable that resolves the current effective mode.

        Resolution order (highest priority first):
          1. `exports/paused.flag` exists → READ_ONLY (kill switch).
          2. `self._mode_override` set → that mode (used by canary
             controller to flip LIVE → CLOSE_ONLY after first fill,
             added 2026-05-05).
          3. `self.settings.bot_mode` → boot-time mode.

        Layered so each higher-priority override fully shadows the
        lower ones. Touch the flag → halt within one bus tick. Set
        the override → mode flips at next gate evaluation. `rm` the
        flag and clear the override → boot mode resumes.
        """
        from poly_terminal.shared.pause_flag import (
            make_pause_aware_mode_getter,
        )

        def _override_or_settings():
            override = getattr(self, "_mode_override", None)
            if override is not None:
                return override
            return self.settings.bot_mode

        return make_pause_aware_mode_getter(_override_or_settings)

    def _construct_live_client_if_possible(self) -> None:
        """Derive L2 creds from the private key + funder and stand up a
        LiveOrderClient. Cached on `self` so UserWebSocket can reuse the
        same creds without paying the derivation cost twice. If the
        private key or funder isn't set, we leave self.live_client=None
        and ExecutionAgent will skip the live branch (PAPER works fine
        without it).
        """
        pk = self.settings.poly_private_key
        funder = self.settings.poly_proxy_address
        if not pk or not funder:
            logger.info(
                "live_client: POLY_PRIVATE_KEY or POLY_PROXY_ADDRESS unset "
                "— live order signing disabled (PAPER-only)"
            )
            return
        derived = derive_l2_creds_from_private_key(
            host=self.settings.clob_api_url,
            private_key=pk,
            funder_address=funder,
        )
        if derived is None:
            logger.warning(
                "live_client: L2 credential derivation failed — "
                "live order signing disabled"
            )
            return
        self._derived_l2_creds = derived
        try:
            self.live_client = LiveOrderClient(
                host=self.settings.clob_api_url,
                private_key=pk,
                funder_address=funder,
                api_key=derived.api_key,
                api_secret=derived.api_secret,
                api_passphrase=derived.api_passphrase,
            )
            logger.info(
                "live_client: ready (mode=%s; LIVE_DRY/LIVE will sign orders)",
                self.settings.bot_mode.value,
            )
        except Exception:
            logger.exception("live_client: construction failed")
            self.live_client = None

    def _build_pipeline(self) -> GatePipeline:
        repo = self.gate_metrics_repo
        assert repo is not None

        async def daily_loss_reader() -> Decimal:
            # Sum today's realized PnL across paper_fills.
            cutoff = int(time.time()) - 86_400
            async with self.db.connect() as conn:
                cur = await conn.execute(
                    "SELECT COALESCE(SUM(realized_pnl), 0) "
                    "FROM paper_fills WHERE filled_at > ?",
                    (cutoff,),
                )
                row = await cur.fetchone()
            return Decimal(str(row[0])) if row else Decimal("0")

        async def open_positions_reader() -> int:
            assert self.positions_repo is not None
            return await self.positions_repo.open_count()

        async def total_exposure_reader() -> Decimal:
            async with self.db.connect() as conn:
                cur = await conn.execute(
                    "SELECT COALESCE(SUM(cost_basis_usd), 0) "
                    "FROM positions WHERE closed_ts IS NULL"
                )
                row = await cur.fetchone()
            return Decimal(str(row[0])) if row else Decimal("0")

        # EntryLiquidityGate orderbook fetch wrappers — bridge the sync
        # py-clob-client-v2 SDK calls into the async gate via to_thread.
        # Skipped (returns None) when live_client is unavailable; the
        # gate then short-circuits with a fetch_failed reject, which is
        # the correct behavior in degraded modes.
        async def _best_ask(token_id: str) -> float | None:
            if self.live_client is None:
                return None
            return await asyncio.to_thread(
                self.live_client.get_best_ask, token_id
            )

        async def _best_bid(token_id: str) -> float | None:
            if self.live_client is None:
                return None
            return await asyncio.to_thread(
                self.live_client.get_best_bid, token_id
            )

        gates = [
            ("mode_lock", ModeLockGate(self._mode_getter())),
            ("duplicate_intent", DuplicateIntentGate()),
        ]
        # EntryLiquidityGate — DISABLED 2026-05-05 (set
        # ENTRY_LIQUIDITY_ENABLED=true to re-enable). Initial deployment
        # at 10% slippage produced a 70-point WR collapse (84% → 13%) on
        # a 114-decided sample over 17min. Hypothesis: the gate's
        # slippage filter introduces ADVERSE SELECTION — accepting only
        # intents where the market has dropped at/below the wallet
        # signal price means the bot enters tokens that are currently
        # falling and likely to keep falling. Need to study the
        # rejected-vs-accepted intent population in the recorder data
        # before deciding whether/how to re-enable.
        if (
            self.live_client is not None
            and os.environ.get("ENTRY_LIQUIDITY_ENABLED", "false").lower() == "true"
        ):
            gates.append((
                "entry_liquidity",
                EntryLiquidityGate(
                    cfg=EntryLiquidityConfig.from_env(),
                    best_ask_fn=_best_ask,
                    best_bid_fn=_best_bid,
                ),
            ))
        gates.extend([
            (
                "per_trade_size",
                # HARD ceiling — strategies use max_position_usd as the
                # soft target and may upsize up to max_position_usd_hard
                # to clear V2's 5-share / $1-marketable floors. The
                # gate enforces the hard line so legitimate elasticity
                # passes while runaway intents still get caught.
                PerTradeSizeGate(self.settings.max_position_usd_hard),
            ),
            ("daily_loss", DailyLossGate(self.settings.daily_loss_cap_usd, daily_loss_reader)),
            (
                "open_positions",
                OpenPositionsGate(
                    self.settings.max_open_positions,
                    open_positions_reader,
                    ledger=getattr(self, "_reservation_ledger", None),
                ),
            ),
            (
                "market_concentration",
                MarketConcentrationGate(
                    self.settings.max_open_positions_per_token,
                    self.positions_repo.count_open_for_token,
                ),
            ),
            # BuyCooldownGate — only meaningful when CashGuard is
            # wired (LIVE mode). When CashGuard isn't present, the
            # getter returns 0 and the gate is a no-op.
            (
                "buy_cooldown",
                BuyCooldownGate(
                    self.cash_guard.get_paused_until
                    if self.cash_guard is not None
                    else (lambda: 0)
                ),
            ),
            (
                "total_exposure",
                TotalExposureGate(
                    self.settings.max_total_exposure_usd, total_exposure_reader
                ),
            ),
            (
                "time_left",
                TimeLeftGate(min_seconds=self.settings.time_left_floor_s),
            ),
            ("latency_budget", LatencyBudgetGate(self.gamma_budget)),
            ("heartbeat_alive", HeartbeatAliveGate(max_age_seconds=30.0)),
        ])
        return GatePipeline(gates, middleware=metrics_middleware(repo))

    async def construct_agents(self) -> None:
        # Exit agent + execution agent first so positions can flow.
        # positions_repo lets ExitAgent rebuild bar_end_ts / _positions
        # from the DB at startup so a restart doesn't leak open
        # positions past their bar resolution.
        # 2026-05-04 shadow-execution: when a tick-starved position
        # hits bar resolution, the bar_watcher previously fell back
        # to entry_price → guaranteed $0 PnL on every flat close. Now
        # it queries the live orderbook for the best contra (best_bid
        # for our BUY positions) and uses that as the shadow exit
        # price. Closes the LIVE_DRY simulation gap that was producing
        # 80%+ flat closes regardless of strategy mix or cap.
        shadow_price_fn = None
        if self.live_client is not None:
            async def _shadow_price(token_id: str, side: str) -> float | None:
                # Closing a BUY position = marketable SELL = fills at
                # best_bid. Closing a SELL = marketable BUY = best_ask.
                # PositionState always passes side="BUY" today.
                if side.upper() == "BUY":
                    return await asyncio.to_thread(
                        self.live_client.get_best_bid, token_id
                    )
                return await asyncio.to_thread(
                    self.live_client.get_best_ask, token_id
                )
            shadow_price_fn = _shadow_price
        self.exit_agent = ExitAgent(
            bus=self.bus,
            positions_repo=self.positions_repo,  # type: ignore[arg-type]
            shadow_price_fn=shadow_price_fn,
            eval_recorder=self.exit_evals_repo,
        )
        # Live order plumbing (wired in construct_live_clients before
        # construct_agents fires for the LIVE_DRY/LIVE paths). For PAPER
        # they stay None and the agent skips the live branch entirely.
        #
        # 2026-05-07 PHASE 18 — wire the trades_fetcher so the SELL
        # error handler can reconcile matching-in-flight responses
        # against on-chain fill data. Without this, the agent treats
        # both "balance: 0" error formats identically (the legacy
        # behavior up through Phase 5 — preserved as fallback).
        trades_fetcher = None
        funder = self.settings.poly_proxy_address
        if self.data_api_client is not None and funder:
            async def _fetch_user_trades(limit: int) -> list[dict[str, Any]]:
                if self.data_api_client is None:
                    return []
                return await self.data_api_client.fetch_user_trades(
                    funder, limit
                )
            trades_fetcher = _fetch_user_trades

        # 2026-05-09 PHASE 31 — reconciliation lock repo, shared by
        # ExecutionAgent (sets lock on SELL_FAILED), PositionImporter
        # (skips locked tokens), and RedeemerAgent (clears lock on
        # successful redeem).
        from poly_terminal.persistence.repositories.reconciliation_locks import (
            ReconciliationLockRepo,
        )
        self.reconciliation_lock_repo = ReconciliationLockRepo(self.db)

        # Phase 32 P2 — on-chain inventory pre-check.
        # Build a CTF reader + bound callable that ExecutionAgent uses
        # before each live SELL submit. Disabled if the proxy address
        # is unset (no funder to query) — make_onchain_inventory_check
        # returns None in that case and the agent treats it as a no-op.
        try:
            _execution_ctf_reader = ctf_reader_from_settings(
                self.settings.polygon_rpc_url_primary,
                self.settings.polygon_rpc_url_fallback,
            )
        except Exception:
            logger.exception(
                "phase32_p2: failed to build CTF reader for execution "
                "pre-check; legacy behavior (no pre-check) will run"
            )
            _execution_ctf_reader = None
        from poly_terminal.agents.execution.agent import (
            make_onchain_inventory_check,
        )
        _onchain_inventory_check = make_onchain_inventory_check(
            ctf_reader=_execution_ctf_reader,
            funder_address=str(self.settings.poly_proxy_address or ""),
        )

        self.execution_agent = ExecutionAgent(
            bus=self.bus,
            fills_repo=self.fills_repo,  # type: ignore[arg-type]
            positions_repo=self.positions_repo,  # type: ignore[arg-type]
            live_orders_repo=self.live_orders_repo,
            live_client=self.live_client,
            # 2026-05-08 PHASE 28 — patient SELL on EXIT_SL.
            # Reads opt-in flag from settings (env: SL_PATIENT_MODE).
            # Default False keeps legacy FAK behavior intact for any
            # operator who doesn't explicitly enable patient mode.
            patient_mode_getter=lambda: bool(self.settings.sl_patient_mode),
            patient_wait_s=int(self.settings.sl_patient_wait_s),
            patient_target=str(self.settings.sl_patient_target),
            patient_min_time_to_close_s=int(
                self.settings.sl_patient_min_time_to_close_s
            ),
            mode_getter=self._mode_getter(),
            trades_fetcher=trades_fetcher,
            # Phase 31 — wire the reconciliation lock repo so SELL
            # escalation-exhausted creates a quarantine.
            reconciliation_lock_repo=self.reconciliation_lock_repo,
            # Phase 31 P1c — production min-hold = 30s. v54 pos 22499
            # hit balance:0 at elapsed≈85s; the legacy 3s gate was too
            # tight for Polymarket V2's settlement window. Tests
            # default to the class constant (3s) which the suite is
            # already calibrated for.
            min_hold_s=ExecutionAgent.PRODUCTION_MIN_HOLD_S,
            # Phase 32 P2 — defer SELL when chain hasn't credited the
            # BUY yet (None when funder/reader missing → no-op).
            onchain_inventory_check=_onchain_inventory_check,
        )
        # Live fill reconciler closes the loop on submitted orders by
        # marking the audit row filled/partial/cancelled when the User
        # WS reports the outcome. Wired regardless of mode — PAPER
        # events are dropped cheaply by the `paper: True` filter.
        if self.live_orders_repo is not None:
            self.live_fill_reconciler = LiveFillReconciler(
                bus=self.bus,
                live_orders_repo=self.live_orders_repo,
                # 2026-05-03: pass positions_repo so unmatched SELL
                # fills (operator manual closes on Polymarket UI)
                # can close the corresponding open position instead
                # of leaving ProfitTaker firing SELLs against zero
                # on-chain inventory.
                positions_repo=self.positions_repo,
            )
        # Bug #2 (cap race) — in-memory ledger of risk-approved BUY intents
        # awaiting fill. The OpenPositionsGate consumes its `.count()`; the
        # RiskAgent reserves on gate-pass and releases on terminal order
        # events. TTL is the safety net for execution paths that don't
        # publish terminal events.
        self._reservation_ledger = OpenPositionsReservationLedger(
            ttl_seconds=30.0,
        )
        self.risk_agent = RiskAgent(
            bus=self.bus,
            buy_pipeline=self._build_pipeline(),
            reservation_ledger=self._reservation_ledger,
        )

        # Intel agents.
        self.orderbook_agent = OrderbookIntelAgent(
            bus=self.bus,
            imbalance_cfg=ImbalanceConfig(
                threshold=Decimal("0.30"), confirmation_bars=3
            ),
        )
        self.context_agent = ContextAgent(
            bus=self.bus, cfg=ContextConfig(min_time_left_s=60)
        )
        self.wallet_agent = WalletIntelAgent(
            bus=self.bus,
            repo=self.wallets_repo,  # type: ignore[arg-type]
            tracked_wallets=set(),
            ranker_cfg=RankerConfig(
                top_pct=self.settings.wallet_top_pct,
                wr_floor=self.settings.wallet_top_decile_floor_win_rate,
                trades_floor=self.settings.wallet_top_decile_trades_30d_floor,
                wallet_followed_override=(
                    frozenset(
                        w.strip().lower()
                        for w in self.settings.wallet_followed_override.split(",")
                        if w.strip()
                    )
                    if self.settings.wallet_followed_override.strip()
                    else None
                ),
            ),
        )

        # Strategies.
        intent_counts: dict[str, int] = {}
        # Phase 34 — narrow Optional[LedgerSnapshotRefresher] to non-None
        # once at the top of the strategy block. setup_db (called before
        # this point in the boot path) constructs it; each
        # `if self.settings.strategy_X:` block below uses
        # `self.ledger_refresher.snapshot` as the snapshot getter.
        assert self.ledger_refresher is not None

        # 2026-05-11 PHASE 38 — RiskAllocator constructed UNCONDITIONALLY
        # before any strategy block. Pre-Phase-38 the allocator was
        # built inside `if strategy_copy_trade:`, which meant any preset
        # that disabled copy_trade but enabled another strategy (e.g.
        # scalp_window_validated) crashed at construct_agents() with
        # `AttributeError: no attribute 'risk_allocator'`. Now the
        # allocator is always available; strategies that opt out of
        # the gate just don't pass it.
        #
        # The LIVE allow-list comes from `build_live_allowed_strategies`
        # (module-level helper, unit-tested). See its docstring for the
        # Phase 41.5 bug that motivated the extraction.
        live_allowed_strategies = build_live_allowed_strategies(
            self.settings,
        )
        bakeoff_active = (
            self.settings.bakeoff_enabled
            and self.settings.bot_mode is BotMode.PAPER
        )
        self.risk_allocator = RiskAllocator(AllocatorConfig(
            bankroll_usd=float(self.settings.max_position_usd_hard) * 4,
            live_position_cap_usd=float(self.settings.max_position_usd_hard),
            open_position_limit=int(self.settings.max_open_positions),
            max_total_exposure_usd=float(
                self.settings.max_total_exposure_usd
            ),
            daily_loss_cap_usd=float(self.settings.daily_loss_cap_usd),
            one_strategy_at_a_time=not bakeoff_active,
            live_allowed=live_allowed_strategies,
            wallet_probation_min_paper_fills=5,
        ))
        if bakeoff_active:
            from pathlib import Path as _Path

            from poly_terminal.agents.strategy.lane_book import (
                LaneBook,
                LaneRealizedCache,
                load_lanes,
            )

            _lanes = load_lanes(
                _Path(__file__).resolve().parents[2]  # noqa: ASYNC240
                / "config" / "bakeoff" / "lanes_v1.yaml"
            )
            self.lane_realized_cache = LaneRealizedCache(
                positions_repo=self.positions_repo,
                lane_ids=[ln.id for ln in _lanes],
            )
            self.lane_book = LaneBook(
                _lanes,
                lane_realized_getter=self.lane_realized_cache.get,
            )
            _scalp_wallets = {
                w.strip().lower()
                for w in self.settings.wallet_copy_scalp_override.split(",")
                if w.strip()
            }
            self._strategies.extend(
                build_bakeoff_strategies(
                    _lanes,
                    bus=self.bus,
                    lane_book=self.lane_book,
                    mode_getter=self._mode_getter(),
                    ledger_snapshot_getter=self.ledger_refresher.snapshot,
                    copy_scalp_wallets=_scalp_wallets,
                )
            )
            for _ln in _lanes:
                if _ln.enabled:
                    intent_counts[_ln.id] = 0
            logger.info(
                "BAKE-OFF active: %d lanes (%d enabled)",
                len(_lanes), sum(1 for x in _lanes if x.enabled),
            )

        if not bakeoff_active and self.settings.strategy_copy_trade:
            # 2026-05-08 PHASE 21 — wire best_ask_getter so the
            # strategy can apply the pre-trade slippage gate. Falls
            # through to None (gate disabled) in PAPER / READ_ONLY
            # where the live client wasn't constructed.
            phase21_ask_getter = (
                self.live_client.get_best_ask
                if self.live_client is not None
                else None
            )
            # Phase 34 (2026-05-11) — DONE. Was: TODO 60s-refresher.
            # Now: LedgerSnapshotRefresher reads positions_repo
            # every 15s and exposes a cached snapshot. Closes the
            # no-op gate hole flagged in deep-research-report (30).
            self.copy_trade = CopyTradeStrategy(
                bus=self.bus,
                cfg=CopyTradeConfig(
                    proportion=Decimal("0.30"),
                    max_position_usd=self.settings.max_position_usd,
                    # Single source of truth — same value reaches both
                    # the strategy (as the elasticity ceiling) and the
                    # PerTradeSizeGate (as the hard reject line). At
                    # soft=$2, hard=$5 the tradeable price band
                    # expands to ≤ ~$0.95 while normal-sized intents
                    # still target $1-$2.
                    max_position_usd_hard=self.settings.max_position_usd_hard,
                    # Re-enabled now that the followed set is broader
                    # (top-decile selection vs single hand-picked
                    # wallet). The aggregate-PAPER backtest showed
                    # p≥$0.80 has 41.7% win rate / -$0.19 avg PnL
                    # across the multi-wallet sample. Drop to None
                    # (or higher) again only when restricting to a
                    # single wallet whose high-price band is
                    # individually validated.
                    max_buy_price=Decimal("0.75"),
                ),
                best_ask_getter=phase21_ask_getter,
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.copy_trade)
            intent_counts["copy_trade"] = 0
        if not bakeoff_active and self.settings.strategy_flash_crash:
            self.flash_crash = FlashCrashStrategy(
                bus=self.bus,
                cfg=FlashCrashConfig(size_usd=self.settings.max_position_usd),
                # Phase 32 P3 — RiskAllocator gate.
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.flash_crash)
            intent_counts["flash_crash"] = 0
        if not bakeoff_active and self.settings.strategy_scalp_15m:
            self.scalp_15m = ScalpWindowStrategy(
                bus=self.bus,
                cfg=ScalpConfig(
                    window="15m",
                    size_usd=self.settings.max_position_usd,
                    # Phase 38 — bleed-band filter (default off via
                    # 0.0/0.0; operator opts in via preset YAML).
                    bleed_band_lo=float(
                        self.settings.scalp_window_bleed_band_lo
                    ),
                    bleed_band_hi=float(
                        self.settings.scalp_window_bleed_band_hi
                    ),
                    # Phase 41.6 — entry-price floor / ceiling (default
                    # off; operator opts in via preset YAML).
                    entry_price_lo=float(
                        self.settings.scalp_window_entry_price_lo
                    ),
                    entry_price_hi=float(
                        self.settings.scalp_window_entry_price_hi
                    ),
                    # Phase 41.8 — min-time-to-resolution gate
                    # (mitigation; default off via 0).
                    min_seconds_to_resolution=int(
                        self.settings.scalp_window_min_seconds_to_resolution
                    ),
                ),
                # Phase 32 P3 — RiskAllocator gate.
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.scalp_15m)
            intent_counts["scalp_15m"] = 0
        if not bakeoff_active and self.settings.strategy_scalp_1h:
            self.scalp_1h = ScalpWindowStrategy(
                bus=self.bus,
                cfg=ScalpConfig(
                    window="1h",
                    size_usd=self.settings.max_position_usd,
                    bleed_band_lo=float(
                        self.settings.scalp_window_bleed_band_lo
                    ),
                    bleed_band_hi=float(
                        self.settings.scalp_window_bleed_band_hi
                    ),
                    entry_price_lo=float(
                        self.settings.scalp_window_entry_price_lo
                    ),
                    entry_price_hi=float(
                        self.settings.scalp_window_entry_price_hi
                    ),
                    # Phase 41.8 — min-time-to-resolution gate
                    # (mitigation; default off via 0).
                    min_seconds_to_resolution=int(
                        self.settings.scalp_window_min_seconds_to_resolution
                    ),
                ),
                # Phase 32 P3 — RiskAllocator gate.
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.scalp_1h)
            intent_counts["scalp_1h"] = 0
        if not bakeoff_active and self.settings.strategy_dump_hedge:
            self.dump_hedge = DumpHedgeStrategy(
                bus=self.bus,
                cfg=DumpHedgeConfig(
                    size_usd=self.settings.max_position_usd,
                    dump_pct=self.settings.strategy_dump_hedge_dump_pct,
                    target_edge_pct=self.settings.strategy_dump_hedge_target_edge_pct,
                ),
                # Phase 32 P3 — RiskAllocator gate.
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.dump_hedge)
            intent_counts["dump_hedge"] = 0
        # 2026-05-09 PHASE 32 P3 — Endgame Yield strategy.
        # Off by default; opt-in via STRATEGY_ENDGAME_YIELD=true and
        # ENDGAME_CONFIDENCE_OVERRIDES=m1:t1:0.97:2,...
        # Per playbook §17 hard rule the strategy stays disabled until
        # PAPER soak + EV gate audit pass.
        if not bakeoff_active and self.settings.strategy_endgame_yield:
            from poly_terminal.agents.strategy.confidence_source import (
                ManualConfidenceSource,
            )
            from poly_terminal.agents.strategy.endgame_evaluator import (
                EndgameMarketEvaluator,
                GammaMarketMeta,
            )
            from poly_terminal.agents.strategy.endgame_yield import (
                EndgameYieldConfig,
                EndgameYieldStrategy,
            )

            self.endgame_confidence_source = ManualConfidenceSource()
            self.endgame_confidence_source.load_from_env_string(
                self.settings.endgame_confidence_overrides or ""
            )

            # 2026-05-10 Phase 32 P3 (item 7) — real sync Gamma fetcher
            # replacing the prior stub. urllib-based, in-memory TTL
            # cache (5min positive / 30s negative), no new top-level
            # deps. Tests mock the transport so no real HTTP fires.
            from poly_terminal.agents.strategy.gamma_metadata_fetcher import (
                GammaMetadataFetcher,
            )
            self.endgame_gamma_fetcher = GammaMetadataFetcher()

            ask_getter = (
                self.live_client.get_best_ask
                if self.live_client is not None else (lambda _t: None)
            )
            bid_getter = (
                self.live_client.get_best_bid
                if self.live_client is not None else (lambda _t: None)
            )
            self.endgame_evaluator = EndgameMarketEvaluator(
                confidence_source=self.endgame_confidence_source,
                gamma_metadata_fetcher=self.endgame_gamma_fetcher.fetch,
                best_ask_getter=ask_getter,
                best_bid_getter=bid_getter,
                depth_getter=None,  # depth gate fail-closed by default
            )
            self.endgame_yield = EndgameYieldStrategy(
                bus=self.bus,
                cfg=EndgameYieldConfig(
                    position_size_usd=float(self.settings.max_position_usd),
                ),
                evaluate_market=self.endgame_evaluator.evaluate,
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.endgame_yield)
            intent_counts["endgame_yield"] = 0
            logger.info(
                "endgame_yield: started — confidence entries=%d, "
                "STRATEGY_ENDGAME_YIELD=true",
                len(self.endgame_confidence_source._entries),
            )
        if not bakeoff_active and self.settings.strategy_copy_scalp:
            scalp_wallets = {
                w.strip().lower()
                for w in self.settings.wallet_copy_scalp_override.split(",")
                if w.strip()
            }
            if not scalp_wallets:
                logger.warning(
                    "strategy_copy_scalp=true but WALLET_COPY_SCALP_OVERRIDE "
                    "is empty — strategy will run as a no-op. Set the env var "
                    "to a comma-separated list of 0x addresses to enable."
                )
            from poly_terminal.agents.strategy.copy_scalp import (
                CopyScalpConfig,
                CopyScalpStrategy,
            )
            # Phase 21 — same slippage gate wiring as copy_trade.
            phase21_scalp_ask_getter = (
                self.live_client.get_best_ask
                if self.live_client is not None
                else None
            )
            self.copy_scalp = CopyScalpStrategy(
                bus=self.bus,
                followed_wallets=scalp_wallets,
                cfg=CopyScalpConfig(),
                best_ask_getter=phase21_scalp_ask_getter,
                # 2026-05-10 Phase 32 P3 — RiskAllocator gate on
                # copy_scalp (parity with copy_trade). Closes the
                # framework-bypass gap documented in
                # docs/strategy_wiring_debt.md.
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.copy_scalp)
            intent_counts["copy_scalp"] = 0
            # Register the scalp wallets with the activity poller so
            # their fills actually reach the bus. Done at strategy
            # construction time so they're polled from the first sweep.
            self._copy_scalp_extra_wallets = scalp_wallets
        else:
            # 2026-05-09 — when STRATEGY_COPY_SCALP=false the attribute
            # was never set, breaking later references like the wallet
            # variance auditor's `self.copy_scalp is not None` check.
            # Pin to None so all downstream code paths are uniform.
            self.copy_scalp = None
            self._copy_scalp_extra_wallets = set()

        if not bakeoff_active and self.settings.strategy_copy_scalp_active:
            active_wallets = {
                w.strip().lower()
                for w in self.settings.wallet_copy_scalp_active_override.split(",")
                if w.strip()
            }
            if not active_wallets:
                logger.warning(
                    "strategy_copy_scalp_active=true but "
                    "WALLET_COPY_SCALP_ACTIVE_OVERRIDE is empty — "
                    "strategy will run as a no-op."
                )
            from poly_terminal.agents.strategy.copy_scalp_active import (
                CopyScalpActiveConfig,
                CopyScalpActiveStrategy,
            )
            active_ask_getter = (
                self.live_client.get_best_ask
                if self.live_client is not None
                else None
            )
            self.copy_scalp_active = CopyScalpActiveStrategy(
                bus=self.bus,
                followed_wallets=active_wallets,
                cfg=CopyScalpActiveConfig(),
                best_ask_getter=active_ask_getter,
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.copy_scalp_active)
            intent_counts["copy_scalp_active"] = 0
            self._copy_scalp_extra_wallets |= active_wallets
        else:
            self.copy_scalp_active = None

        # 2026-05-11 PHASE 37 — Crypto Bar Momentum strategy (option-A
        # counterpart to endgame_yield, for Polymarket 5-15 min crypto
        # Up/Down bars). Default OFF; scaffold only — fail-closed at
        # the signal level because `stub_momentum_score` always
        # returns 0.0. Wiring is plumbing-complete so a real signal
        # can be dropped in without re-architecting.
        if not bakeoff_active and self.settings.strategy_crypto_bar_momentum:
            from poly_terminal.agents.strategy.crypto_bar_momentum import (
                BarCandidate,
                CryptoBarMomentumConfig,
                CryptoBarMomentumStrategy,
                stub_momentum_score,
            )

            def _stub_evaluator(_market_id: str, _token_id: str):
                """Scaffold evaluator — returns None for every market.
                The real evaluator (when wired) reads:
                  - market_id + token_id → market metadata cache
                  - last N seconds of ticks → momentum_score
                  - top-of-book → yes_price / no_price / spread
                And returns a populated BarCandidate or None.
                """
                return None

            self.crypto_bar_momentum = CryptoBarMomentumStrategy(
                bus=self.bus,
                cfg=CryptoBarMomentumConfig(),
                evaluate_bar_at=_stub_evaluator,
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.crypto_bar_momentum)
            intent_counts["crypto_bar_momentum"] = 0
            logger.warning(
                "crypto_bar_momentum: scaffold-only wiring — stub "
                "evaluator returns no candidates and stub momentum "
                "score is 0.0. Strategy is fail-closed; calibrate "
                "before expecting fills.",
            )
        else:
            self.crypto_bar_momentum = None

        # 2026-05-12 PHASE 39 — Certainty Farm ("99-cent farming").
        # Counterpart to endgame_yield with stricter defaults baked in
        # per deep-research-report 33/34 recommendations. Same
        # operator-curated confidence pattern (CERTAINTY_FARM_OVERRIDES
        # env). Default OFF; fail-closed when no overrides configured.
        if not bakeoff_active and self.settings.strategy_certainty_farm:
            from poly_terminal.agents.strategy.certainty_farm import (
                CertaintyFarmConfig,
                CertaintyFarmStrategy,
                FarmCandidate,
            )
            from poly_terminal.agents.strategy.confidence_source import (
                ManualConfidenceSource,
            )
            from poly_terminal.agents.strategy.endgame_evaluator import (
                EndgameMarketEvaluator,
            )
            from poly_terminal.agents.strategy.gamma_metadata_fetcher import (
                GammaMetadataFetcher,
            )

            # Operator-curated overrides (separate pool from endgame_yield).
            self.certainty_farm_confidence_source = ManualConfidenceSource()
            self.certainty_farm_confidence_source.load_from_env_string(
                self.settings.certainty_farm_overrides or "",
            )

            # Reuse a Gamma fetcher — share one if endgame_yield is also
            # enabled (avoid double-caching), else create a fresh one.
            cf_gamma_fetcher = getattr(
                self, "endgame_gamma_fetcher", None,
            ) or GammaMetadataFetcher()
            # Persist so future agents (and tests) can reach it.
            if not hasattr(self, "endgame_gamma_fetcher"):
                self.endgame_gamma_fetcher = cf_gamma_fetcher

            # Reuse same orderbook getters as endgame_yield. live_client
            # is None in pure-PAPER deploys without signing keys; the
            # lambdas keep the evaluator from crashing in that case.
            cf_ask = (
                self.live_client.get_best_ask
                if self.live_client is not None else (lambda _t: None)
            )
            cf_bid = (
                self.live_client.get_best_bid
                if self.live_client is not None else (lambda _t: None)
            )
            # Compose a dedicated EndgameMarketEvaluator instance for
            # certainty_farm — it's the same plumbing but pointed at
            # certainty_farm's OWN confidence source so the two
            # strategies' override pools stay separate.
            self.certainty_farm_evaluator = EndgameMarketEvaluator(
                confidence_source=self.certainty_farm_confidence_source,
                gamma_metadata_fetcher=cf_gamma_fetcher.fetch,
                best_ask_getter=cf_ask,
                best_bid_getter=cf_bid,
                depth_getter=None,
            )

            def _certainty_evaluator(
                market_id: str, _token_id: str,
            ) -> FarmCandidate | None:
                """Adapter: delegates to EndgameMarketEvaluator (which
                picks the right side based on which token has a
                confidence override) and converts its EndgameCandidate
                into a FarmCandidate. Returns None when the evaluator
                returns None (no override / missing book / missing
                Gamma) — fail-closed."""
                ec = self.certainty_farm_evaluator.evaluate(market_id)
                if ec is None:
                    return None
                return FarmCandidate(
                    market_id=ec.market_id,
                    token_id=ec.token_id,
                    side=ec.side,
                    entry_price=ec.entry_price,
                    spread=ec.spread,
                    time_to_close_s=ec.time_to_close_s,
                    true_p=ec.confidence.true_p,
                    source_count=ec.confidence.sources_count,
                )

            self.certainty_farm = CertaintyFarmStrategy(
                bus=self.bus,
                cfg=CertaintyFarmConfig(),
                evaluate_candidate_at=_certainty_evaluator,
                # 2026-05-12 — tick-driven flow. Bind the reverse
                # lookup so the strategy only evaluates ticks for
                # tokens that have an operator override. Cheap O(1)
                # check on every market tick.
                find_market_for_token=(
                    self.certainty_farm_confidence_source.find_market_id_for_token
                ),
                allocator=self.risk_allocator,
                mode_getter=self._mode_getter(),
                ledger_snapshot_getter=self.ledger_refresher.snapshot,
            )
            self._strategies.append(self.certainty_farm)
            intent_counts["certainty_farm"] = 0
            n_overrides = (
                len(self.certainty_farm_confidence_source._entries)
                if hasattr(self.certainty_farm_confidence_source, "_entries")
                else 0
            )
            logger.info(
                "certainty_farm: started with %d confidence override(s); "
                "evaluator wired through EndgameMarketEvaluator "
                "(orderbook + Gamma + confidence). Strategy is "
                "fail-closed when no overrides configured.",
                n_overrides,
            )
        else:
            self.certainty_farm = None

        # 2026-05-12 PHASE 40 — Last Stretch Farming (RESEARCH ONLY).
        # The strategy module enforces 3 of 4 safety layers itself:
        #   - hard PAPER-only check at construction (raises on LIVE)
        #   - research_armed=True required at construction (raises if False)
        #   - gate stack rejects anything outside [0.95, 0.99] × [10s, 90s]
        # This wiring block adds the 4th layer: the strategy is ONLY
        # constructed when BOTH STRATEGY_LAST_STRETCH_FARMING=true AND
        # LAST_STRETCH_RESEARCH_ARMED=true. Either flag off → no
        # instantiation, no telemetry, no risk.
        if (
            self.settings.strategy_last_stretch_farming
            and self.settings.last_stretch_research_armed
        ):
            from poly_terminal.agents.strategy.last_stretch_farming import (
                LastStretchConfig,
                LastStretchError,
                LastStretchFarmingStrategy,
            )

            def _ttc_stub(_token_id: str) -> int | None:
                """Production TTC getter would consult the watchlist or
                market metadata cache. Stubbed for now — strategy stays
                silent until a real TTC source is wired."""
                return None

            try:
                self.last_stretch = LastStretchFarmingStrategy(
                    bus=self.bus,
                    cfg=LastStretchConfig(
                        price_lo=float(self.settings.last_stretch_price_lo),
                        price_hi=float(self.settings.last_stretch_price_hi),
                        ttc_min_s=int(self.settings.last_stretch_ttc_min_s),
                        ttc_max_s=int(self.settings.last_stretch_ttc_max_s),
                    ),
                    research_armed=True,
                    mode_getter=self._mode_getter(),
                    ttc_getter=_ttc_stub,
                    allocator=self.risk_allocator,
                    ledger_snapshot_getter=self.ledger_refresher.snapshot,
                )
                self._strategies.append(self.last_stretch)
                intent_counts["last_stretch_farming"] = 0
                logger.warning(
                    "last_stretch_farming: ⚠️ RESEARCH SCAFFOLD started. "
                    "Empirical lifetime ROI in this band is -1.42%% across "
                    "367 positions. TTC getter is stubbed → strategy will "
                    "emit ZERO intents in production until a real TTC "
                    "source is wired. Strategy refuses to construct "
                    "outside PAPER mode."
                )
            except LastStretchError as exc:
                logger.error(
                    "last_stretch_farming: construction REFUSED — %s", exc,
                )
                self.last_stretch = None
        else:
            self.last_stretch = None

        # WebSockets — constructed but not yet running.
        if self.settings.enable_market_websocket:
            self.market_ws = MarketWebSocket(
                bus=self.bus,
                url=f"{self.settings.clob_ws_url}/ws/market",
            )

            async def _on_watchlist(_e: str, payload: object) -> None:
                # Discovery (when wired) publishes EVT_WATCHLIST_UPDATED with a
                # markets list; subscribe to all token IDs from each market.
                if not isinstance(payload, dict):
                    return
                tokens: list[str] = []
                for m in payload.get("markets", []) or []:
                    for t in (m.get("token_yes"), m.get("token_no")):
                        if t:
                            tokens.append(str(t))
                if tokens and self.market_ws is not None:
                    self.market_ws.subscribe_tokens(tokens)

            self.bus.subscribe(EVT_WATCHLIST_UPDATED, _on_watchlist)

            # 2026-05-02: auto-subscribe to tokens the followed whales
            # are trading. Pre-fix, the market WS only watched
            # Discovery's universe (BTC/ETH bars), so OrderbookIntel
            # had zero coverage of the politics/sports/news markets the
            # whales actually trade — copy_trade's imbalance gate
            # silently dropped ~80% of intents. Now the first wallet
            # fill on a new token bootstraps a book subscription so
            # subsequent fills on the same token can earn an imbalance
            # boost. Idempotent — subscribe_tokens dedupes inside
            # MarketWebSocket.
            _seen_subs: set[str] = set()

            async def _on_wallet_fill_subscribe(_e: str, payload: object) -> None:
                if not isinstance(payload, dict) or self.market_ws is None:
                    return
                token = str(payload.get("token_id", ""))
                if not token or token in _seen_subs:
                    return
                _seen_subs.add(token)
                try:
                    self.market_ws.subscribe_tokens([token])
                except Exception:
                    logger.exception(
                        "market_ws: subscribe failed for wallet-fill "
                        "token %s", token,
                    )

            self.bus.subscribe(EVT_WALLET_FILL, _on_wallet_fill_subscribe)

        if self.settings.enable_user_websocket:
            # Reuse the L2 creds the LiveOrderClient already derived
            # at boot. Falls back to env-pinned creds if no private key
            # was configured (read-only smoke-test mode).
            api_key = self.settings.poly_api_key
            api_secret = self.settings.poly_api_secret
            api_passphrase = self.settings.poly_api_passphrase
            if self._derived_l2_creds is not None:
                api_key = self._derived_l2_creds.api_key
                api_secret = self._derived_l2_creds.api_secret
                api_passphrase = self._derived_l2_creds.api_passphrase
                logger.info(
                    "ws.user: reusing fresh L2 creds derived at boot"
                )
            self.user_ws = UserWebSocket(
                bus=self.bus,
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
                funder_address=self.settings.poly_proxy_address,
                tracked_wallets=set(),  # populated after seed_tracked_wallets_from_repo
                url=f"{self.settings.clob_ws_url}/ws/user",
            )

            async def _on_rank(_e: str, payload: object) -> None:
                if isinstance(payload, dict) and self.user_ws is not None:
                    self.user_ws.update_tracked_wallets(
                        {w for w in payload.get("followed", set())}
                    )

            self.bus.subscribe(EVT_WALLET_RANK_CHANGED, _on_rank)

        # Discovery — finds live BTC/ETH 15m + 1h markets every 60s.
        # Uses the public Gamma API (no auth) and publishes
        # EVT_WATCHLIST_UPDATED, which the Market WS subscribe-handler
        # (above) consumes to subscribe to the new YES/NO token IDs.
        self.gamma_client = GammaClient(
            base_url=self.settings.gamma_api_url,
            budget=self.gamma_budget,
        )
        self.discovery = DiscoveryAgent(
            bus=self.bus,
            gamma=self.gamma_client,
            cfg=DiscoveryConfig(
                assets=("btc", "eth"),
                windows=("15m", "1h"),
                interval_s=60,
            ),
        )

        # WalletActivityPoller — Polymarket's User WS streams events only
        # for the authenticated user, so to copy-trade arbitrary whales we
        # poll Data API /activity per followed wallet on a tight cadence.
        self.data_api_client = DataApiClient(
            base_url=self.settings.data_api_url,
            budget=self.data_api_budget,
        )
        self.activity_poller = WalletActivityPoller(
            bus=self.bus,
            data_api=self.data_api_client,
            cfg=PollerConfig(interval_s=1.0, limit_per_wallet=20),  # Option C: 3s→1s
        )

        # Option A: Polygon on-chain OrderFilled subscriber (~2-5s lag vs 30s API).
        # Option B: CLOB best_ask surge detector (~0-500ms lag).
        # Both are constructed here; wallets/tokens are seeded in _start_background_tasks
        # once wallet_agent and strategy wallets are resolved.
        self.polygon_log_sub = PolygonLogSubscriber(
            bus=self.bus,
            # Default public Polygon WS node — no config key needed.
            rpc_ws_url="wss://polygon.publicnode.com",
            gamma_url=self.settings.gamma_api_url,
        )
        self.price_surge_detector = PriceSurgeDetector(
            bus=self.bus,
            surge_threshold_pct=3.0,
            cooldown_s=10.0,
        )

        # PositionImporter — discovers on-chain positions opened
        # outside the bot (manual UI trades, prior-run leftovers).
        # LIVE/LIVE_DRY/CLOSE_ONLY only — PAPER has no on-chain holdings.
        if (
            self.settings.bot_mode in (
                BotMode.LIVE, BotMode.LIVE_DRY, BotMode.CLOSE_ONLY,
            )
            and self.settings.poly_proxy_address
        ):
            from poly_terminal.data.data_api.positions import PositionsClient
            # 2026-05-06 PHASE 8 — pass the data-api fetch_activity
            # method into the importer so its delta-sweep can recover
            # actual SELL fill prices when shares vanish off-chain
            # (out-of-band SELL retries, manual UI closes, etc.).
            self.position_importer = PositionImporterAgent(
                bus=self.bus,
                positions_source=PositionsClient(self.data_api_client),
                positions_repo=self.positions_repo,
                live_orders_repo=self.live_orders_repo,
                funder_address=self.settings.poly_proxy_address,
                market_ws=self.market_ws,
                cfg=PositionImporterConfig(),
                activity_fetcher=self.data_api_client.fetch_activity,
                # Phase 31 — skip imports for tokens with active
                # reconciliation locks (set by ExecutionAgent on
                # terminal SELL_FAILED).
                reconciliation_lock_repo=self.reconciliation_lock_repo,
            )

        # 2026-05-07 PHASE 10 — HeldTokenSubscriberAgent (deep-research-23
        # item #4). Subscribes to EVT_POSITION_OPENED and calls
        # market_ws.subscribe_tokens([token_id]) so newly-opened
        # positions on tokens NOT in the recorder's pre-subscribed
        # set still receive WS ticks. Without this, ProfitTaker /
        # ExitDecisionEngine cannot evaluate TP/SL and every position
        # rides to bar resolution. Wired in any non-READ_ONLY mode
        # since the bus event flow is identical across PAPER/LIVE_DRY/
        # LIVE/CLOSE_ONLY — the subscribe is idempotent + no-ops when
        # market_ws is None (PAPER tests).
        if self.settings.bot_mode is not BotMode.READ_ONLY:
            self.held_token_subscriber = HeldTokenSubscriberAgent(
                bus=self.bus,
                ws=self.market_ws,
            )

        # 2026-05-08 PHASE 30(a) — OrdersRecorderAgent.
        # Subscribes to user-channel order events and persists them
        # to the `orders` table. Bus-only — safe to wire in any non-
        # READ_ONLY mode. PAPER mode rarely produces real user-channel
        # events (no live submits), but the agent is harmless there.
        if self.settings.bot_mode is not BotMode.READ_ONLY:
            self.orders_recorder = OrdersRecorderAgent(
                bus=self.bus,
                repo=OrdersRepo(self.db),
            )

        # 2026-05-08 PHASE 30(b) — FreshnessTracker.
        # In-memory observability surface. Subscribes to position
        # open/close + market ticks + exit-eval events; surfaces a
        # snapshot via /api/freshness for the operator.
        # Always wired (read-only by design; no DB writes).
        self.freshness_tracker = FreshnessTracker(bus=self.bus)

        # Redeemer agent — only meaningful when there are closed
        # positions to settle. Safe to run in PAPER too: WORTHLESS
        # auto-marks are just SQL updates and REDEEMABLE total just
        # tells the operator there's money to claim. Skip when
        # mode=READ_ONLY since the bot can't have any positions
        # there.
        # ProfitTaker is bus-only (no DB / no network) — safe to wire
        # in any non-READ_ONLY mode. Fires EVT_SELL_INTENT which is
        # already handled by ExecutionAgent. Symmetric: profit AND
        # loss thresholds default to 10¢/$1.
        if self.settings.bot_mode is not BotMode.READ_ONLY:
            # 2026-05-02 round-trip-economics retune.
            # Old defaults (TP=10%, SL=10%, escalator 5×5%=23%
            # worst-case slippage) made round-trips break-even at
            # best — typical 2-retry SELL turned a +10% TP into a
            # net -1% loss. New defaults absorb expected escalator
            # slippage with margin:
            #   TP=15%   ← 2-retry escalator at 2% leaves +10.7%
            #   SL=7%    ← cut losers faster (was bleeding -10% + slip)
            #   trail_arm=15%, trail_lock=8%, trail_giveback=30%
            #     ← stay comfortably above breakeven post-slippage
            # Combined with execution-agent escalator change to
            # 3×2% (max 6% slip), gross +15% TP nets ≥ +9% even
            # in the worst-case escalator path.
            # 2026-05-08 PHASE 24 retune.
            #   profit_threshold: 0.15 → 0.05  (lock quick wins)
            #   trail_arm:       0.15 → 0.05  (track the new TP)
            #   trail_lock_pct:  0.08 → 0.03  (floor stays below arm)
            # Backtest: 97-position retro showed +5% TP would have
            # produced $+8.55 cumulative vs actual $+4.10 (+108%).
            # 8% / 10% gradient review queued for the next cron.
            #
            # loss_threshold left at -7% per backtest finding that
            # tighter SL was net-negative (-$2.04 to -$2.31 vs actual)
            # and looser SL was even worse (-$7.36).
            # 2026-05-08 PHASE 24.5 — TP dollar floor. User-observed
            # ~$0.20 round-trip drag (gas + relayer + spread).
            # tp_floor_usd=$0.40 covers drag with a 100% safety
            # margin, blocking 5% TP fires on small positions where
            # absolute gain wouldn't cover drag (net-losing trades).
            self.profit_taker = ProfitTakerAgent(
                bus=self.bus,
                cfg=ProfitTakerConfig(
                    profit_threshold_per_dollar=Decimal("0.05"),
                    loss_threshold_per_dollar=Decimal("0.07"),
                    trail_arm_per_dollar=Decimal("0.05"),
                    trail_lock_pct=Decimal("0.03"),
                    tp_floor_usd=Decimal("0.40"),
                ),
                eval_recorder=self.exit_evals_repo,
            )
            # AutoTuner observes rolling PnL and hot-swaps the
            # ProfitTaker thresholds. Bus-only — no extra network or
            # DB writes beyond a periodic positions read.
            # 2026-05-08 PHASE 24: defaults + min floors aligned with
            # the new TP=5% baseline. min_profit=0.04 lets the tuner
            # adapt down 1pp from the new baseline before bottoming.
            self.auto_tuner = AutoTunerAgent(
                bus=self.bus,
                profit_taker=self.profit_taker,
                positions_repo=self.positions_repo,
                cfg=AutoTunerConfig(
                    default_profit_threshold=Decimal("0.05"),
                    default_loss_threshold=Decimal("0.07"),
                    min_profit_threshold=Decimal("0.04"),
                    min_loss_threshold=Decimal("0.05"),
                ),
            )
            # CashGuard pauses new BUYs (SELLs unaffected) when
            # cumulative session PnL crosses +$10. Cooldown until
            # top of the next hour.
            self.cash_guard = CashGuardAgent(
                bus=self.bus,
                positions_repo=self.positions_repo,
                cfg=CashGuardConfig(profit_threshold_usd=10.0),
            )
            # SessionGuard: writes the kill-switch flag when cumulative
            # realized PnL crosses ±$20. LIVE / CLOSE_ONLY only — both
            # touch real money so cumulative loss protection matters.
            # PAPER PnL accumulates fast and would auto-pause the
            # operator's observation runs.
            if self.settings.bot_mode in (BotMode.LIVE, BotMode.CLOSE_ONLY):
                self.session_guard = SessionGuardAgent(
                    bus=self.bus,
                    cfg=SessionGuardConfig(
                        profit_target_usd=20.0,
                        loss_limit_usd=20.0,
                    ),
                )
            # 2026-05-05 CanaryController — only constructed when
            # boot mode is LIVE (the only mode where a canary BUY can
            # actually fill on-chain). On first LIVE fill, sets
            # `self._mode_override = CLOSE_ONLY` so subsequent BUYs
            # are blocked at the mode_lock gate but in-flight SELLs
            # continue to flow.
            if self.settings.bot_mode is BotMode.LIVE:
                from poly_terminal.agents.canary_controller.agent import (
                    CanaryControllerAgent,
                )
                assert self.live_orders_repo is not None

                def _flip_to_close_only() -> None:
                    self._mode_override = BotMode.CLOSE_ONLY

                self.canary_controller = CanaryControllerAgent(
                    bus=self.bus,
                    mode_getter=lambda: self.settings.bot_mode,
                    live_orders_repo=self.live_orders_repo,
                    on_canary_fired=_flip_to_close_only,
                )

        if self.settings.bot_mode is not BotMode.READ_ONLY:
            self.redeemer_resolver = GammaMarketResolver()
            # 2026-05-07 PHASE 16 — optional auto-redeem submitter.
            # Constructed only when the operator opts in via
            # `REDEEMER_AUTO_ENABLED=true`. The redeemer's own
            # dry_run flag is wired separately so the operator can
            # flip auto-on while keeping submission off (calldata
            # logging only) for a verification sweep before going
            # live with real funds.
            relayer_redeemer = None
            if self.settings.redeemer_auto_enabled:
                from poly_terminal.data.clob.redeem import (
                    RelayerCreds,
                    RelayerRedeemer,
                )
                # Builder Codes are required for the relayer endpoint
                # — distinct from the CLOB L2 keys above. Refuse to
                # construct the redeemer in live mode without them
                # so a misconfigured deploy fails noisily on boot
                # rather than silently 401-ing every sweep.
                missing = [
                    name for name, v in (
                        ("POLY_BUILDER_API_KEY",
                         self.settings.poly_builder_api_key),
                        ("POLY_BUILDER_API_SECRET",
                         self.settings.poly_builder_api_secret),
                        ("POLY_BUILDER_API_PASSPHRASE",
                         self.settings.poly_builder_api_passphrase),
                    ) if not v
                ]
                if missing and not self.settings.redeemer_dry_run:
                    logger.error(
                        "redeemer: REDEEMER_AUTO_ENABLED=true and "
                        "REDEEMER_DRY_RUN=false but missing %s — "
                        "create Builder Codes via Polymarket UI "
                        "(Settings > Builder Codes). Auto-redeem "
                        "DISABLED to prevent silent 401 spam.",
                        ", ".join(missing),
                    )
                else:
                    try:
                        relayer_redeemer = RelayerRedeemer(
                            creds=RelayerCreds(
                                private_key=self.settings.poly_private_key,
                                funder_address=self.settings.poly_proxy_address,
                                signature_type=1,  # Magic Link / proxy
                                builder_api_key=self.settings.poly_builder_api_key,
                                builder_secret=self.settings.poly_builder_api_secret,
                                builder_passphrase=self.settings.poly_builder_api_passphrase,
                            ),
                            dry_run=self.settings.redeemer_dry_run,
                        )
                        logger.info(
                            "redeemer: auto-redeem WIRED (dry_run=%s) — "
                            "first sweep will %s",
                            self.settings.redeemer_dry_run,
                            "log calldata only"
                            if self.settings.redeemer_dry_run
                            else "submit on-chain via relayer-v2",
                        )
                    except Exception:
                        logger.exception(
                            "redeemer: failed to construct RelayerRedeemer "
                            "(falling back to manual-claim mode)"
                        )
                        relayer_redeemer = None
            self.redeemer_agent = RedeemerAgent(
                positions_repo=self.positions_repo,
                market_resolver=self.redeemer_resolver,
                cfg=RedeemerConfig(
                    auto_enabled=self.settings.redeemer_auto_enabled,
                    # Phase 32 — opt-in WORTHLESS_NO_TX escalation for
                    # low-payout positions stuck at retry cap.
                    worthless_no_tx_after_cap=(
                        self.settings.redeemer_worthless_no_tx_after_cap
                    ),
                    worthless_no_tx_payout_ceiling_usd=(
                        self.settings.redeemer_worthless_no_tx_payout_ceiling_usd
                    ),
                    # Phase 33 — PAPER positions go through Gamma
                    # resolution + truth-up so soak realized_pnl
                    # matches what real money would book.
                    paper_truth_up_enabled=(
                        self.settings.redeemer_paper_truth_up_enabled
                    ),
                ),
                # Inventory gate: positions never backed by a
                # successful LIVE BUY (PAPER, rejected BUYs, resting
                # unfilled BUYs) get auto-marked PAPER_NO_TX so
                # phantom paper $ doesn't show up as REDEEMABLE.
                live_orders_repo=self.live_orders_repo,
                relayer_redeemer=relayer_redeemer,
                # Phase 31 — clear reconciliation lock on successful
                # redeem so the importer can resume normal behavior.
                reconciliation_lock_repo=self.reconciliation_lock_repo,
            )

        # Build monitor state with everything wired.
        self.monitor_state = MonitorState(
            db=self.db,
            fills_repo=self.fills_repo,
            positions_repo=self.positions_repo,
            wallets_repo=self.wallets_repo,
            gate_metrics_repo=self.gate_metrics_repo,
            exit_evals_repo=self.exit_evals_repo,
            wallet_agent=self.wallet_agent,
            risk_agent=self.risk_agent,
            exit_agent=self.exit_agent,
            redeemer_agent=self.redeemer_agent,
            inventory_report=self.inventory_report,
            freshness_tracker=self.freshness_tracker,
            config_fingerprint=compute_fingerprint(dict(os.environ)),
            bot_mode=self.settings.bot_mode.value,
            started_at=int(time.time()),
            agent_heartbeat={},
            strategy_intent_counts=intent_counts,
            latency_budgets={
                "gamma": self.gamma_budget.summary,
                "clob_book": self.clob_budget.summary,
                "data_api": self.data_api_budget.summary,
            },
        )

    async def seed_tracked_wallets_from_repo(self, top_n: int = 100) -> int:
        """Populate `wallet_agent.tracked` from the top wallets in `wallet_scores`.

        Called at boot so the User WebSocket dispatcher fans in fills for
        wallets the operator (or a CSV seeder, or a prior leaderboard
        sync) has already loaded into the DB. Without this, tracked is
        empty forever and copy_trade can't fire.

        Filtered by `settings.wallet_preferred_category` when set (default:
        'crypto'). Empty string means "all categories".
        """
        assert self.wallet_agent is not None
        assert self.wallets_repo is not None
        category = self.settings.wallet_preferred_category or None
        scores = await self.wallets_repo.fetch_top(
            limit=top_n, category=category
        )
        wallets = {s.wallet for s in scores}
        self.wallet_agent.set_tracked(wallets)
        return len(wallets)

    async def reconcile_inventory_or_fail(self) -> None:
        """Item #2: DB ↔ on-chain CTF balanceOf gate.

        Runs in LIVE / LIVE_DRY / CLOSE_ONLY only. PAPER mode skips
        (no live exposure to misalign). The reconciler queries
        `balanceOf(proxy_wallet, token_id)` on the Polygon CTF
        contract for every open DB position and fail-fast on drift.

        Why hard-fail rather than auto-correct: if the DB and chain
        disagree, ExitAgent could submit SELLs against zero on-chain
        inventory (the canary scenario in reverse). Refusing to start
        forces the operator to triage the disagreement before any
        further trades fly.

        PAPER soft mode: when `bot_mode == PAPER` we skip entirely.
        Tests + dev: hard_gate is forced False if no proxy_wallet —
        the reconciler logs the report and returns without raising.
        """
        from poly_terminal.shared.enums import BotMode

        if self.settings.bot_mode == BotMode.PAPER:
            logger.info(
                "inventory_reconciler: skipped (mode=PAPER, no live exposure)"
            )
            return
        if self.settings.bot_mode == BotMode.READ_ONLY:
            logger.info(
                "inventory_reconciler: skipped (mode=READ_ONLY)"
            )
            return
        proxy = self.settings.poly_proxy_address.strip()
        if not proxy:
            logger.warning(
                "inventory_reconciler: POLY_PROXY_ADDRESS unset; "
                "skipping (cannot query on-chain without it)"
            )
            return
        assert self.positions_repo is not None
        reader = ctf_reader_from_settings(
            self.settings.polygon_rpc_url_primary,
            self.settings.polygon_rpc_url_fallback,
        )
        # Hard gate ONLY in LIVE / CLOSE_ONLY — those modes have real
        # on-chain positions where DB-vs-chain drift is a bug.
        # LIVE_DRY positions are SHADOW (orders signed but never
        # submitted), so by definition every DB row has 0 on-chain
        # inventory. Running the reconciler in LIVE_DRY would
        # always hard-fail. Use soft mode in LIVE_DRY so we still
        # log the report (catches imported-position drift) without
        # blocking boot. 2026-05-06 fix after first bounce-attempt
        # exposed this exact mismatch (24/24 LIVE_DRY shadow tokens
        # showed 100% drift; refusing to start was correct gate
        # behaviour but wrong policy for shadow mode).
        is_real_money_mode = self.settings.bot_mode in (
            BotMode.LIVE,
            BotMode.CLOSE_ONLY,
        )
        self.inventory_reconciler = InventoryReconcilerAgent(
            cfg=InventoryReconcilerConfig(
                proxy_wallet=proxy,
                hard_gate=is_real_money_mode,
            ),
            positions_repo=self.positions_repo,
            ctf_reader=reader,
        )
        # In LIVE / CLOSE_ONLY: InventoryDriftError propagates and
        # aborts boot. In LIVE_DRY: returns the report without
        # raising; we log the summary so operators see drift counts
        # but don't block the soak.
        report = await self.inventory_reconciler.run()
        self.inventory_report = report
        logger.info(
            "inventory_reconciler: %s gate=%s",
            report.summary(),
            "HARD" if is_real_money_mode else "SOFT(LIVE_DRY)",
        )

    async def start_agents(self) -> None:
        assert self.exit_agent is not None
        assert self.execution_agent is not None
        assert self.risk_agent is not None
        assert self.orderbook_agent is not None
        assert self.context_agent is not None
        assert self.wallet_agent is not None
        # Phase 34 (2026-05-11) — prime the ledger snapshot cache + spawn
        # its 15s refresh loop BEFORE any strategy starts evaluating
        # intents. Without this, the first intents in the boot window
        # would read an empty snapshot and bypass the position/exposure
        # gates until the loop's first tick — small window but
        # operationally important on a freshly-restarted bot with
        # carried-over open positions.
        if self.ledger_refresher is not None:
            await self.ledger_refresher.start()
        if self.lane_realized_cache is not None:
            await self.lane_realized_cache.start()
        await self.exit_agent.start()
        await self.execution_agent.start()
        if self.live_fill_reconciler is not None:
            await self.live_fill_reconciler.start()
        await self.risk_agent.start()
        await self.orderbook_agent.start()
        await self.wallet_agent.start()
        if self.profit_taker is not None:
            await self.profit_taker.start()
        if self.auto_tuner is not None:
            await self.auto_tuner.start()
        if self.cash_guard is not None:
            await self.cash_guard.start()
        if self.session_guard is not None:
            await self.session_guard.start()
        if self.canary_controller is not None:
            await self.canary_controller.start()
        if self.position_importer is not None:
            await self.position_importer.start()
        # 2026-05-07 PHASE 10 — start the held-token WS subscriber.
        # Subscribed to EVT_POSITION_OPENED; on each open it calls
        # market_ws.subscribe_tokens([token_id]) so the WS feeds
        # ticks to ProfitTaker + ExitDecisionEngine.
        if self.held_token_subscriber is not None:
            await self.held_token_subscriber.start()
        if self.redeemer_agent is not None:
            await self.redeemer_agent.start()
        # 2026-05-08 PHASE 30 — order audit trail + freshness tracker.
        if self.orders_recorder is not None:
            await self.orders_recorder.start()
        if self.freshness_tracker is not None:
            await self.freshness_tracker.start()

        # 2026-05-08 PHASE 29(b) — orphan GTC cancel-on-boot.
        # Wipe any leftover resting GTC orders from a previous run so
        # Polymarket V2's share-collateral lock doesn't block new
        # patient-SELL submissions. Default OFF — opt in via env.
        # Only meaningful in LIVE / CLOSE_ONLY where live_client exists.
        if (
            self.settings.boot_cancel_orphan_gtc
            and self.live_client is not None
        ):
            try:
                cancelled = await self.live_client.cancel_all_orders()
                if cancelled:
                    logger.warning(
                        "phase29b: cancelled %d orphan GTC order(s) at "
                        "boot (BOOT_CANCEL_ORPHAN_GTC=true)", cancelled,
                    )
                else:
                    logger.info(
                        "phase29b: no orphan GTC orders to cancel at boot",
                    )
            except Exception:
                logger.exception(
                    "phase29b: cancel_all_orders raised at boot — "
                    "any orphan GTC orders may still hold collateral",
                )

        for s in self._strategies:
            await s.start()
        # Seed tracked wallets from any rows already in wallet_scores
        # (CSV seeder, prior boot's leaderboard sync, etc.). At boot we
        # rank using the EXISTING seeded scores; we do NOT re-score from
        # history, because cold-start history is empty and would zero out
        # whatever the seeder put there. The periodic task does the full
        # score+rank once enough history has accumulated.
        seeded = await self.seed_tracked_wallets_from_repo()
        if seeded:
            category = self.settings.wallet_preferred_category or None
            scores = await self.wallets_repo.fetch_top(
                limit=seeded, category=category
            )
            await self.wallet_agent._ranker.refresh(scores)  # type: ignore[attr-defined]
            # Mirror the followed set onto the User WS dispatcher so
            # incoming trades are filtered to the same set the strategy
            # cares about.
            if self.user_ws is not None:
                self.user_ws.update_tracked_wallets(
                    set(self.wallet_agent.followed_wallets)
                )
            logger.info(
                "seeded %d tracked wallets; %d in followed set",
                seeded,
                len(self.wallet_agent.followed_wallets),
            )

        # 2026-05-08 PHASE 23(c) — wallet variance audit (boot-time).
        # Non-blocking: spawns a task that polls /activity for each
        # followed wallet, computes rolling avg PnL per dollar, and
        # pushes the result to copy_trade.set_wallet_avg_pnl (and
        # copy_scalp's, if active). Wallets with negative avg get
        # demoted by the variance gate inside CopyTradeStrategy.
        # Fails open per-wallet — any HTTP / parse error is logged
        # and the wallet is skipped (no avg pushed → gate stays open).
        if self.copy_trade is not None or self.copy_scalp is not None:
            from poly_terminal.agents.strategy.wallet_variance_auditor import (
                audit_wallets_into,
            )
            audit_wallets = (
                set(self.wallet_agent.followed_wallets)
                | set(getattr(self, "_copy_scalp_extra_wallets", set()))
            )
            audit_targets = [
                t for t in (self.copy_trade, self.copy_scalp)
                if t is not None
            ]
            if audit_wallets and audit_targets:
                asyncio.create_task(
                    audit_wallets_into(audit_wallets, audit_targets)
                )

        # Spawn WebSocket + Discovery run tasks.
        if self.market_ws is not None:
            asyncio.create_task(self.market_ws.run(self.shutdown))
        if self.user_ws is not None:
            asyncio.create_task(self.user_ws.run(self.shutdown))
        if self.discovery is not None:
            asyncio.create_task(self.discovery.run(self.shutdown))
        if self.activity_poller is not None:
            # Seed with the boot-time followed set so the very first poll
            # iteration covers known whales (don't wait for the next rank
            # event).
            assert self.wallet_agent is not None
            # Union of (rank-derived copy_trade followed set) +
            # (CopyScalp's static override set) so all configured
            # wallets get the fast-tier 3s poll.
            scalp_extras = set(
                getattr(self, "_copy_scalp_extra_wallets", set())
            )
            followed = set(self.wallet_agent.followed_wallets) | scalp_extras
            self.activity_poller.set_followed(followed)
            # Re-merge after every rank change too — activity_poller's
            # built-in _on_rank handler OVERWRITES the followed set,
            # which would drop the scalp extras on the next rank event.
            # This wrapper subscribes AFTER and re-applies the merge.
            if scalp_extras:
                from poly_terminal.bus.events import EVT_WALLET_RANK_CHANGED

                async def _re_merge_scalp_extras(_e: str, payload: object) -> None:
                    if not isinstance(payload, dict) or self.activity_poller is None:
                        return
                    new_followed = {w for w in payload.get("followed", set())}
                    self.activity_poller.set_followed(new_followed | scalp_extras)

                self.bus.subscribe(EVT_WALLET_RANK_CHANGED, _re_merge_scalp_extras)
            # Slow-tier: tracked-but-not-followed wallets get polled at
            # ~60s cadence to accumulate wallet_history toward the
            # win-rate floor. Without this seed the unfollowed tier
            # never earns into followed.
            self.activity_poller.set_tracked(
                {w for w in self.wallet_agent._tracked}  # type: ignore[attr-defined]
            )
            asyncio.create_task(self.activity_poller.run(self.shutdown))

        # Options A + B: Polygon log subscriber and price-surge detector.
        # Wire them with the same followed set used by activity_poller, then
        # subscribe EVT_WALLET_FILL so any fill (from poller OR log_sub) seeds
        # the surge detector's watch set for that token.
        if self.polygon_log_sub is not None:
            _all_followed: set[str] = set()
            if self.wallet_agent is not None:
                _all_followed |= set(self.wallet_agent.followed_wallets)
            _all_followed |= set(getattr(self, "_copy_scalp_extra_wallets", set()))
            self.polygon_log_sub.set_followed(_all_followed)
            asyncio.create_task(self.polygon_log_sub.run(self.shutdown))

        if self.price_surge_detector is not None:
            await self.price_surge_detector.start()

            async def _seed_surge_watch(_e: str, payload: object) -> None:
                if not isinstance(payload, dict) or self.price_surge_detector is None:
                    return
                token_id = str(payload.get("token_id", ""))
                market_id = str(payload.get("market_id", ""))
                if token_id and market_id:
                    self.price_surge_detector.watch_token(token_id, market_id)

            self.bus.subscribe(EVT_WALLET_FILL, _seed_surge_watch)

        # Bar-resolution watcher — closes positions whose underlying short-
        # window bar has settled even if no further price tick arrives.
        # Critical for whale-copy on 15m / 1h crypto markets.
        assert self.exit_agent is not None
        asyncio.create_task(self.exit_agent.run_bar_watcher(self.shutdown))

        # 2026-05-04 patch: WS subscription re-sync watchdog.
        # The auto-subscribe-on-EVT_WALLET_FILL handler can miss tokens
        # (race at boot, lost wallet-fill event during reconnect, etc.)
        # leaving open positions without market ticks → bar_watcher
        # falls back to entry_price → 100% flat closes (the regression
        # observed 2026-05-04). This task periodically reads every
        # open-position token from the DB and pushes them to the
        # MarketWS SubscriptionManager. SubscriptionManager dedupes
        # internally so this is cheap to run frequently.
        async def ws_resync_watchdog() -> None:
            # 2026-05-04 report (22) recommendation: 30s→5s. Tighter
            # cadence closes the window where a position opens and bar-
            # resolves before the watchdog re-subscribes. SubscriptionManager
            # dedupes internally so this is cheap to run frequently.
            interval_s = 5.0
            assert self.positions_repo is not None
            while not self.shutdown.is_set():
                if self.market_ws is not None:
                    try:
                        tokens = await self.positions_repo.fetch_all_open_token_ids()
                        if tokens:
                            self.market_ws.subscribe_tokens(tokens)
                    except Exception:
                        logger.exception(
                            "ws_resync_watchdog: failed to re-sync subscriptions"
                        )
                try:
                    await asyncio.wait_for(self.shutdown.wait(), timeout=interval_s)
                    return
                except asyncio.TimeoutError:
                    continue
        asyncio.create_task(ws_resync_watchdog())

        # 2026-05-05 TickPoller (REST fallback) — synthesizes
        # EVT_MARKET_TICK from /price polls when the WS feed is silent.
        # Polymarket's /ws/market started failing to deliver
        # price_change events around 06:00 today; without ticks the
        # ExitDecisionEngine's TP/SL/time-stop branches sit idle and
        # every position rides to bar resolution. The poller fills the
        # gap by hitting REST every N seconds for each open-position
        # token.
        #
        # 2026-05-07 PHASE 11: switched gate from os.environ to
        # self.settings (pydantic-loaded). Pre-fix, .env's
        # TICK_POLLER_ENABLED=true never reached os.environ — the
        # poller was silently disabled. Pos 22356 (canary v13) showed
        # the impact: only 1 exit_eval (bar_watcher) for a 6-min
        # position because WS was silent AND TickPoller wasn't
        # actually running.
        #
        # No-op when no live_client (PAPER builds without L1 wallet).
        if (
            self.settings.tick_poller_enabled
            and self.live_client is not None
            and self.positions_repo is not None
        ):
            from poly_terminal.agents.tick_poller.agent import TickPoller

            poll_interval_s = self.settings.tick_poller_interval_s
            per_call_timeout_s = self.settings.tick_poller_timeout_s
            assert self.live_client is not None
            live = self.live_client
            assert self.positions_repo is not None
            repo = self.positions_repo
            self.tick_poller = TickPoller(
                bus=self.bus,
                get_best_bid=live.get_best_bid,
                get_best_ask=live.get_best_ask,
                get_last_trade_price=live.get_last_trade_price,
                get_open_tokens=repo.fetch_all_open_token_ids,
                poll_interval_s=poll_interval_s,
                per_call_timeout_s=per_call_timeout_s,
            )
            asyncio.create_task(self.tick_poller.run(self.shutdown))
            logger.info(
                "tick_poller: enabled (interval=%.1fs timeout=%.1fs) "
                "[Phase 11 settings-gated]",
                poll_interval_s, per_call_timeout_s,
            )
        else:
            self.tick_poller = None
            logger.info(
                "tick_poller: disabled (settings.tick_poller_enabled="
                "%s, live_client_set=%s, positions_repo_set=%s)",
                self.settings.tick_poller_enabled,
                self.live_client is not None,
                self.positions_repo is not None,
            )

        # Heartbeat refresher task.
        async def heartbeat() -> None:
            while not self.shutdown.is_set():
                now = int(time.time())
                assert self.monitor_state is not None
                self.monitor_state.agent_heartbeat = {
                    "exit": now,
                    "execution": now,
                    "risk": now,
                    "orderbook": now,
                    "wallet": now,
                    **{s.name: now for s in self._strategies},
                }
                # Refresh per-strategy intent counts.
                self.monitor_state.strategy_intent_counts = {
                    s.name: int(s.intents_emitted) for s in self._strategies
                }
                await self.bus.publish(EVT_AGENT_HEARTBEAT, {"ts": now})
                try:
                    await asyncio.wait_for(self.shutdown.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
        asyncio.create_task(heartbeat())

        # Periodic wallet refresh task. Sync the leaderboard (no-op if
        # leaderboard_sync isn't wired), recompute scores from history,
        # re-seed the tracked set so newly-promoted wallets show up.
        # Waits ONE interval before first fire so boot-time seeded scores
        # (CSV or otherwise) survive long enough for fills to accumulate.
        async def wallet_refresh() -> None:
            interval_s = 3_600  # 1h
            while not self.shutdown.is_set():
                try:
                    await asyncio.wait_for(self.shutdown.wait(), timeout=interval_s)
                    return  # shutdown fired
                except asyncio.TimeoutError:
                    pass
                try:
                    assert self.wallet_agent is not None
                    await self.wallet_agent.sync_leaderboard()
                    await self.wallet_agent.refresh_scores_and_rank()
                    n = await self.seed_tracked_wallets_from_repo()
                    if self.activity_poller is not None:
                        # Re-seed the slow tier so newly-tracked wallets
                        # start accumulating history immediately.
                        self.activity_poller.set_tracked(
                            {w for w in self.wallet_agent._tracked}  # type: ignore[attr-defined]
                        )
                    logger.info(
                        "wallet refresh: %d tracked, %d followed",
                        n,
                        len(self.wallet_agent.followed_wallets),
                    )
                except Exception:
                    logger.exception("wallet refresh failed")
        asyncio.create_task(wallet_refresh())

    async def run_monitor(self) -> None:
        app = build_app(self.monitor_state)
        config = uvicorn.Config(
            app,
            host=self.settings.monitor_host,
            port=self.settings.monitor_port,
            log_level=self.settings.log_level.lower(),
            access_log=False,
        )
        server = uvicorn.Server(config)
        await server.serve()

    async def shutdown_gracefully(self) -> None:
        logger.info("shutdown requested")
        self.shutdown.set()
        # Phase 34 — explicitly stop the ledger refresher so its
        # background task cancels cleanly. Without this, the task
        # would still get cancelled by asyncio teardown, but with a
        # CancelledError that would noise the shutdown log.
        if self.ledger_refresher is not None:
            try:
                await self.ledger_refresher.stop()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "shutdown: ledger_refresher.stop() raised: %s", exc,
                )
        if self.lane_realized_cache is not None:
            try:
                await self.lane_realized_cache.stop()
            except Exception as exc:
                logger.warning(
                    "shutdown: lane_realized_cache.stop() raised: %s", exc,
                )


def install_safety_locks() -> None:
    """Force RC defaults into env BEFORE Settings instantiation.

    Per ADR + docs/06 §A.5 — every boot starts at READ_ONLY/PAPER/disarmed
    regardless of file content. Operators must use scripts/promote.py and
    boot with `--allow-mode <X>` to advance the mode.
    """
    os.environ["BOT_MODE"] = "READ_ONLY"
    os.environ["PAPER_MODE"] = "true"
    os.environ["ARMED"] = "false"


_PROMOTION_MAX_AGE_SECONDS = 24 * 3600


async def _verify_promotion(db: Database, requested: str) -> tuple[bool, str]:
    """Return (ok, detail). Latest promotion must match `requested` AND be
    within `_PROMOTION_MAX_AGE_SECONDS`."""
    from poly_terminal.persistence.repositories.mode_promotions import (
        ModePromotionsRepo,
    )

    latest = await ModePromotionsRepo(db).latest()
    if latest is None:
        return False, "no promotion record — run scripts/promote.py first"
    if latest.to_mode != requested:
        return (
            False,
            f"latest promotion is {latest.to_mode!r}, requested {requested!r}",
        )
    age = int(time.time()) - latest.ts
    if age > _PROMOTION_MAX_AGE_SECONDS:
        return False, f"promotion is {age}s old (>{_PROMOTION_MAX_AGE_SECONDS}s)"
    return True, f"promotion #{latest.promotion_id} signed by {latest.signed_by}"


def _apply_allowed_mode(mode: str) -> None:
    """Set env vars to match an allowed promotion BEFORE Settings construction."""
    os.environ["BOT_MODE"] = mode
    if mode == "READ_ONLY":
        os.environ["PAPER_MODE"] = "true"
        os.environ["ARMED"] = "false"
    elif mode == "PAPER":
        os.environ["PAPER_MODE"] = "true"
        os.environ["ARMED"] = "true"
    elif mode in ("LIVE_DRY", "LIVE"):
        os.environ["PAPER_MODE"] = "false"
        os.environ["ARMED"] = "true"


async def _async_main(argv: list[str] | None = None) -> int:
    _python_version_guard()

    import argparse

    parser = argparse.ArgumentParser(prog="poly-terminal")
    parser.add_argument(
        "--allow-mode",
        choices=("READ_ONLY", "PAPER", "LIVE_DRY", "LIVE"),
        default=None,
        help=(
            "Lift the READ_ONLY safety lock to this mode. Requires a "
            "matching, recent (<24h) row in mode_promotions written by "
            "scripts/promote.py."
        ),
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    # Phase 1: preset overlay (sets env from PARAMS_PRESET BEFORE Settings).
    try:
        apply_preset_to_env()
    except Exception as exc:  # pragma: no cover — defensive
        print(f"[STARTUP] preset overlay failed: {exc}", file=sys.stderr)

    # Phase 2: preflight is the canonical drift gate. We re-invoke it
    # in-process so the operator gets the same exit codes.
    rc = preflight_main()
    if rc != 0:
        return rc

    # Phase 3: lock safety BEFORE constructing Settings.
    install_safety_locks()

    # Phase 3b: optionally lift the lock to a verified, recent promotion.
    if args.allow_mode is not None and args.allow_mode != "READ_ONLY":
        bootstrap_settings = Settings(_env_file=None)
        bootstrap_db = Database(bootstrap_settings.db_path)
        await bootstrap_db.initialize()
        ok, detail = await _verify_promotion(bootstrap_db, args.allow_mode)
        if not ok:
            print(
                f"[STARTUP] --allow-mode {args.allow_mode} rejected: {detail}",
                file=sys.stderr,
            )
            return 2
        _apply_allowed_mode(args.allow_mode)
        print(f"[STARTUP] mode lifted to {args.allow_mode} ({detail})")

    settings = Settings()
    _configure_logging(settings.log_level, settings.log_format)
    logger.info(
        "starting poly_terminal mode=%s paper=%s armed=%s",
        settings.bot_mode.value,
        settings.paper_mode,
        settings.armed,
    )

    terminal = PolyTerminal(settings)
    await terminal.initialize()
    # Item #2: DB ↔ on-chain CTF balanceOf gate. Runs after the DB +
    # live client are initialized but BEFORE any agent constructs or
    # starts — a drift here aborts boot with a clear operator message.
    # PAPER / READ_ONLY are skipped inside the method (no live exposure).
    await terminal.reconcile_inventory_or_fail()
    await terminal.construct_agents()
    await terminal.start_agents()

    # Wire signals.
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(
                getattr(signal, sig_name),
                lambda: asyncio.create_task(terminal.shutdown_gracefully()),
            )
        except NotImplementedError:
            pass

    monitor_task = asyncio.create_task(terminal.run_monitor())

    try:
        await terminal.shutdown.wait()
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return asyncio.run(_async_main(argv))
    except SystemExit as exc:
        return int(exc.code or 0)
    except Exception as exc:
        logger.exception("startup error")
        print(f"[STARTUP] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
