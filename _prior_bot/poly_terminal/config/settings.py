"""Pydantic Settings — single source of truth for runtime configuration.

Precedence (highest first):
  1. Shell env
  2. .env (loaded by pydantic-settings unless `_env_file=None`)
  3. PARAMS_PRESET overlay (applied by `preset_loader.apply_to_env`)
  4. Defaults declared on this class

Risk-critical fields (see ADR 0004) are listed in `RISK_CRITICAL_KEYS` and the
preflight script fails-fast on drift between the resolved preset and `.env`.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Final

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from poly_terminal.shared.enums import BotMode

# Order-executing modes (real-money / signed-order paths). Single
# source of truth shared by the mode-lock + bake-off validators so a
# future mode addition can't silently desync the two safety gates.
_EXECUTING_MODES: frozenset[BotMode] = frozenset(
    {BotMode.LIVE, BotMode.LIVE_DRY, BotMode.CLOSE_ONLY}
)

RISK_CRITICAL_KEYS: Final[frozenset[str]] = frozenset(
    # `PARAMS_PRESET` is the *selector*, not a tunable — it is recorded
    # separately in the fingerprint payload but is not drift-checked
    # against itself. (A preset cannot reference its own name.)
    {
        "BOT_MODE",
        "PAPER_MODE",
        "ARMED",
        "MAX_POSITION_USD",
        "MAX_POSITION_USD_HARD",
        "MAX_TOTAL_EXPOSURE_USD",
        "MAX_OPEN_POSITIONS",
        "DAILY_LOSS_CAP_USD",
        "BAKEOFF_ENABLED",
        "STRATEGY_COPY_TRADE",
        "STRATEGY_FLASH_CRASH",
        "STRATEGY_SCALP_15M",
        "STRATEGY_SCALP_1H",
        "STRATEGY_DUMP_HEDGE",
        "WALLET_TOP_DECILE_FLOOR_WIN_RATE",
        "WALLET_TOP_DECILE_TRADES_30D_FLOOR",
        "STRATEGY_DUMP_HEDGE_DUMP_PCT",
        "STRATEGY_DUMP_HEDGE_TARGET_EDGE_PCT",
    }
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Mode locks (RC) ────────────────────────────────────────────────
    bot_mode: BotMode = BotMode.READ_ONLY
    paper_mode: bool = True
    armed: bool = False

    # ── Preset overlay name ────────────────────────────────────────────
    params_preset: str = "paper_safe"

    # ── Bake-off harness (RC) ──────────────────────────────────────────
    # 2026-05-16 — when true AND bot_mode==PAPER, the parallel strategy
    # bake-off is active: one_strategy_at_a_time is lifted and lanes
    # from config/bakeoff/lanes_v1.yaml run with isolated virtual
    # capital. Hard PAPER-only invariant enforced below. RC: changes the
    # entire allocation regime; preflight must catch drift.
    bakeoff_enabled: bool = False

    # ── Polymarket auth ────────────────────────────────────────────────
    poly_private_key: str = ""
    poly_proxy_address: str = ""
    poly_api_key: str = ""
    poly_api_secret: str = ""
    poly_api_passphrase: str = ""

    # ── External services ──────────────────────────────────────────────
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com"
    polygon_rpc_url_primary: str = ""
    polygon_rpc_url_fallback: str = ""
    coingecko_api_key: str = ""

    # ── Per-trade caps (RC) ────────────────────────────────────────────
    max_position_usd: Decimal = Field(default=Decimal("10"), gt=0)
    # Hard absolute cap. The strategy uses `max_position_usd` as the
    # SOFT target (typical sizing) and may upsize up to this hard
    # ceiling when V2's 5-share / $1-marketable floor would otherwise
    # force the intent to be dropped. PerTradeSizeGate enforces this
    # hard ceiling — anything emitted above it is rejected. Setting
    # equal to `max_position_usd` disables the elasticity (legacy
    # strict-cap behavior). RC because the gate's actual reject line
    # changes when this changes; preflight fingerprint must catch drift.
    max_position_usd_hard: Decimal = Field(default=Decimal("10"), gt=0)
    max_total_exposure_usd: Decimal = Field(default=Decimal("100"), gt=0)
    max_open_positions: int = Field(default=10, gt=0)
    # Cap how many positions can be open on the SAME outcome token
    # (market+side). Without this, copy_trade can stack 6+ positions
    # on a single Bitcoin up/down market within a minute when many
    # followed wallets fire on the same signal — observed live
    # 2026-05-02 with 6 BUYs piling onto "BTC Up or Down 12PM ET".
    # Not RC (operator-tunable; doesn't change overall risk caps).
    max_open_positions_per_token: int = Field(default=3, gt=0)
    daily_loss_cap_usd: Decimal = Field(default=Decimal("15"), gt=0)

    # ── Strategy enable flags (RC) ─────────────────────────────────────
    strategy_copy_trade: bool = True
    strategy_flash_crash: bool = False
    strategy_scalp_15m: bool = False
    strategy_scalp_1h: bool = False
    # 2026-05-11 PHASE 38 — scalp_window entry-price bleed-band filter.
    # When lo<hi, ticks whose price lies in [lo, hi] are rejected. The
    # 2026-05-11 drilldown identified [0.60, 0.80] as a -2.09% ROI
    # bleed zone within scalp_window's otherwise +3.12% lifetime ROI;
    # an operator who wants the bleed filtered enables it via preset.
    # Default lo=hi=0.0 means the filter is OFF.
    scalp_window_bleed_band_lo: float = 0.0
    scalp_window_bleed_band_hi: float = 0.0
    # 2026-05-14 PHASE 41.6 — entry-price band floor / ceiling. Reject
    # ticks whose price is BELOW `entry_price_lo` or ABOVE
    # `entry_price_hi`. Symmetric counterpart to the bleed-band (which
    # blocks a middle zone); this blocks the extreme tails. Motivated
    # by the 2026-05-13→14 overnight where a NO bought at 0.07 with
    # 10min to bar close was truth-up'd to -$5 by Phase 33. Default
    # lo=hi=0.0 → gate OFF; opt in via preset/env.
    scalp_window_entry_price_lo: float = 0.0
    scalp_window_entry_price_hi: float = 0.0
    # 2026-05-16 PHASE 41.8 — min-time-to-resolution entry gate
    # (MITIGATION for the paper-sim fictional-exit pathology, not the
    # root-cause cure). 0 = off. >600 effectively disables 15m-bar
    # entries by construction — see ScalpConfig docstring.
    scalp_window_min_seconds_to_resolution: int = 0
    strategy_dump_hedge: bool = False
    # RC-tunable DumpHedgeConfig thresholds (ADR 0004; defaults match dump_hedge.py).
    strategy_dump_hedge_dump_pct: Decimal = Field(default=Decimal("0.15"), gt=0)
    strategy_dump_hedge_target_edge_pct: Decimal = Field(default=Decimal("0.05"), gt=0)
    # CopyScalp: wallet-signal entry but scalp-style exit (10min hold,
    # 7% SL / 10% TP). Independent wallet override
    # (`wallet_copy_scalp_override`) so it can target high-frequency
    # micro-traders that wouldn't qualify for the long-hold copy_trade
    # decile. RC because changing the entry-signal source materially
    # alters risk profile.
    strategy_copy_scalp: bool = False
    # copy_scalp_active — same exit profile as copy_scalp but targets
    # WALLET_COPY_SCALP_ACTIVE_OVERRIDE (high-volume leaderboard cohort).
    # Independent flag so the two cohorts can be compared side-by-side.
    strategy_copy_scalp_active: bool = False
    # 2026-05-09 PHASE 32 P3 — Endgame Yield strategy.
    # Buys high-probability outcomes converging toward 1.00 within
    # the price band [0.88, 0.97] with verified true_p >= break_even
    # + 3% margin. Default OFF per playbook §17 — must pass PAPER
    # soak + EV gate audit before LIVE re-arm.
    strategy_endgame_yield: bool = False
    # Operator-curated `(market_id, token_id, true_p, sources_count)`
    # entries for ManualConfidenceSource. Format: comma-separated rows
    #   m1:t1:0.97:2,m2:t2:0.92:3
    # Empty by default — endgame_yield emits no intents until at
    # least one entry is configured.
    endgame_confidence_overrides: str = ""

    # 2026-05-11 PHASE 37 — Crypto Bar Momentum strategy.
    # Short-bar (30s-300s) crypto Up/Down counterpart to endgame_yield.
    # Default OFF; the strategy is also fail-closed at the signal level
    # because `stub_momentum_score` returns 0.0 until calibrated.
    # See `docs/strategy_rebuild_2026-05-11.md` §4 option-A.
    strategy_crypto_bar_momentum: bool = False
    # 2026-05-12 PHASE 39 — Certainty Farm ("99-cent farming").
    # High-certainty premium capture with strict gates baked in per
    # deep-research-report 33/34: price ∈ [0.90, 0.97], spread ≤ 0.02,
    # ttc ∈ [15min, 48h], source_count ≥ 2, EV margin ≥ 3%, $1 size.
    # Fail-closed when CERTAINTY_FARM_OVERRIDES is empty.
    strategy_certainty_farm: bool = False
    # Operator-curated `(market_id, token_id, true_p, source_count)`
    # entries for ManualConfidenceSource. Format: comma-separated rows
    # `m1:t1:0.97:2,m2:t2:0.96:3`. Empty by default — strategy emits
    # zero intents until at least one entry is configured.
    certainty_farm_overrides: str = ""
    # 2026-05-12 PHASE 40 — Last Stretch Farming (RESEARCH ONLY).
    # Targets 0.95-0.99 band on 5-min crypto bars. Empirically loses
    # money (367 lifetime positions = -1.42% ROI). Built as a research
    # scaffold; refuses to construct outside PAPER mode AND requires
    # explicit research arming via LAST_STRETCH_RESEARCH_ARMED=true.
    # See `docs/strategies_considered.md`.
    strategy_last_stretch_farming: bool = False
    last_stretch_research_armed: bool = False
    last_stretch_price_lo: float = 0.95
    last_stretch_price_hi: float = 0.99
    last_stretch_ttc_min_s: int = 10
    last_stretch_ttc_max_s: int = 90

    # ── Wallet rank gate (RC) ──────────────────────────────────────────
    wallet_top_decile_floor_win_rate: float = Field(default=0.60, ge=0.0, le=1.0)
    wallet_top_decile_trades_30d_floor: int = Field(default=10, ge=0)
    # Top-N percentile of qualified wallets to admit into the followed
    # set. Default 0.10 (top decile) — operators tune wider when only a
    # handful of wallets are passing the floor and copy_trade isn't
    # producing enough fills. Not RC.
    wallet_top_pct: float = Field(default=0.10, gt=0.0, le=1.0)
    # Preferred category to follow ('' = all, 'crypto' = crypto only, etc.).
    # Not in RISK_CRITICAL_KEYS — operators tune this without re-promoting.
    wallet_preferred_category: str = "crypto"
    # Explicit wallet allowlist (comma-separated 0x… addresses,
    # case-insensitive). When non-empty, the ranker IGNORES top_pct
    # / wr_floor / trades_floor and follows ONLY these wallets.
    # Use for canary / single-wallet test runs. Empty = use the
    # normal top-N% selection. Not RC (operator-tunable).
    wallet_followed_override: str = ""
    # Independent wallet allowlist for the CopyScalp strategy. Empty
    # disables CopyScalp's signal source even when strategy_copy_scalp=True
    # (loud no-op — copy_scalp without wallets is a degenerate config).
    # Comma-separated 0x… addresses. Polled by the WalletActivityPoller
    # in addition to the copy_trade followed set.
    wallet_copy_scalp_override: str = ""
    # Leaderboard wallet cohort for copy_scalp_active. Comma-separated
    # 0x addresses. Polled by WalletActivityPoller alongside the other sets.
    wallet_copy_scalp_active_override: str = ""

    # ── TimeLeft gate floor (seconds) ──────────────────────────────────
    # Tunable so operators can relax the strict 60s default for
    # short-window markets where whales legitimately enter near bar end.
    time_left_floor_s: int = 60

    # ── Latency budgets (ms p95) ───────────────────────────────────────
    latency_budget_gamma_ms: int = 1000
    latency_budget_clob_book_ms: int = 300
    latency_budget_intent_decision_ms: int = 250
    latency_budget_tick_decision_ms: int = 100

    # ── Storage ────────────────────────────────────────────────────────
    db_path: Path = Path("exports/state.db")
    audit_log_dir: Path = Path("exports/audit")

    # ── Monitor ────────────────────────────────────────────────────────
    monitor_host: str = "127.0.0.1"
    monitor_port: int = Field(default=8080, gt=0, lt=65536)

    # ── WebSockets ─────────────────────────────────────────────────────
    enable_market_websocket: bool = True
    enable_user_websocket: bool = True

    # ── TickPoller (REST fallback when WS is silent) ──────────────────
    # 2026-05-07 PHASE 11: previously gated on os.environ.get(...) but
    # .env values are loaded into pydantic Settings, NOT os.environ.
    # Result: TickPoller was silently disabled even when .env had it
    # ON. Pos 22356 (canary v13) had only 1 exit_eval (bar_watcher) —
    # WS subscribed via Phase 10 but Polymarket didn't deliver ticks,
    # and the REST fallback never ran. Read from Settings now.
    tick_poller_enabled: bool = False
    tick_poller_interval_s: float = 5.0
    tick_poller_timeout_s: float = 3.0

    # ── Phase 28 (2026-05-08) — patient SELL on EXIT_SL ────────────────
    # When True, the execution agent's EXIT_SL flow first attempts a GTC
    # SELL at a "scalp" price (default = last_trade_price) and waits
    # `sl_patient_wait_s` seconds before falling through to the legacy
    # FAK aggressive bid-cross. Captures the v44 operator-observed
    # "place a higher SELL and wait for someone to take it" pattern.
    #
    # Default OFF — opt-in via env so the behavior change can be
    # validated in PAPER / LIVE_DRY first. When True, it ALSO requires
    # `time_to_close >= sl_patient_min_time_to_close_s` to engage; on
    # short-bar markets near resolution, falls straight through to FAK.
    sl_patient_mode: bool = False
    sl_patient_wait_s: int = 30
    sl_patient_target: str = "last_trade"  # "last_trade"|"best_ask"|"midpoint"
    sl_patient_min_time_to_close_s: int = 600

    # 2026-05-08 PHASE 29(b) — orphan GTC cancel-on-boot.
    # When True, the bot calls `live_client.cancel_all_orders()` once
    # at startup to wipe any GTC orders left over from a previous run
    # (e.g., patient SELLs that timed out without cleanly cancelling
    # before the bot was killed). Polymarket V2 reserves share
    # collateral on resting GTC orders; orphans block new GTC submits
    # with "balance / active orders" errors. Default OFF — opt in
    # via env so the operator confirms there are no resting orders
    # they want to keep.
    boot_cancel_orphan_gtc: bool = False

    # ── Redeemer auto-redeem (Phase 16, 2026-05-07) ────────────────────
    # Two independent gates that BOTH must be flipped before any real
    # on-chain transaction is submitted:
    #
    #   redeemer_auto_enabled=False (default)
    #     → RedeemerAgent does the legacy "log + accumulate" only.
    #       Calldata is never built; the relayer client is never
    #       imported. Operator manually claims via Polymarket UI.
    #
    #   redeemer_auto_enabled=True + redeemer_dry_run=True
    #     → Calldata is built, fully validated, and pretty-printed
    #       to logs. The position is marked redeemed with the
    #       sentinel "DRY_RUN_REDEEM:<cid>" so the agent doesn't
    #       loop on the same row, but NO transaction is submitted.
    #       Use this state for the first sweep after wiring to
    #       eyeball the calldata against Polymarket UI's call.
    #
    #   redeemer_auto_enabled=True + redeemer_dry_run=False
    #     → REAL on-chain submission via the Polymarket relayer.
    #       Tx hash returned and persisted in
    #       positions.redeem_tx_hash. Real funds move.
    #
    # See `data/clob/redeem.py` and `agents/redeemer/agent.py` for
    # the integration. Stage-2 deploy plan (May 7, 2026): Stage 2a
    # flips auto_enabled=True with dry_run=True to verify calldata,
    # Stage 2b flips dry_run=False on a single small position, then
    # Stage 2c leaves both on for the agent's regular sweeps.
    redeemer_auto_enabled: bool = False
    redeemer_dry_run: bool = True
    # ── Phase 32 (2026-05-09) — WORTHLESS_NO_TX retry-cap fallback ───
    # When True AND auto_enabled is True, positions stuck at the
    # auto_max_retries_per_position cap with payout < ceiling get
    # auto-marked WORTHLESS_NO_TX so Phase 30 truth-up records the
    # cost-basis loss instead of leaving the row stuck REDEEMABLE.
    # Default OFF; flip on for the single-wallet canary phase only.
    redeemer_worthless_no_tx_after_cap: bool = False
    redeemer_worthless_no_tx_payout_ceiling_usd: float = 1.00
    # ── Phase 33 (2026-05-10) — PAPER truth-up after market resolution
    # When True, PAPER positions (no on-chain inventory) still go
    # through Gamma resolution. realized_pnl is truth-up'd to
    # payout_usd − cost_basis when it's approximately zero (silent-loss
    # signature). Closes the soak gap exposed by 2026-05-09 14h soak
    # (positions 22650 + 22736: $6.74 of unrecorded loss).
    redeemer_paper_truth_up_enabled: bool = True
    # ── Builder API credentials for the Polymarket relayer ────────────
    # 2026-05-07: Builder API keys are NOT the same as CLOB L2 keys.
    # The L2 keys (POLY_API_KEY/SECRET/PASSPHRASE) authenticate
    # /order endpoints; they're derived from the L1 private key and
    # rotate freely. Builder Codes are a separate program — created
    # via Polymarket UI under Settings > Builder Codes — and are the
    # only credential the relayer-v2 endpoint accepts. Keep them
    # distinct in env so a misconfiguration produces a clear 401
    # instead of a confusing partial-auth failure.
    #
    # To populate: profile > Settings > Builder Codes > Create
    # Builder Profile > Create New Key. Returns key/secret/passphrase.
    poly_builder_api_key: str = ""
    poly_builder_api_secret: str = ""
    poly_builder_api_passphrase: str = ""

    # ── Logging ────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: str = "json"

    # ── Cross-field validators ─────────────────────────────────────────
    @model_validator(mode="after")
    def _enforce_mode_lock(self) -> "Settings":
        """LIVE / LIVE_DRY require armed=True AND paper_mode=False.

        See ADR 0001 + docs/02_V3_ARCHITECTURE.md §5.
        """
        # 2026-05-05: CLOSE_ONLY also touches real money (SELL submission)
        # so it carries the same ARMED / PAPER_MODE invariants as LIVE.
        if self.bot_mode in _EXECUTING_MODES:
            if not self.armed:
                msg = f"BOT_MODE={self.bot_mode.value} requires ARMED=true"
                raise ValueError(msg)
            if self.paper_mode:
                msg = f"BOT_MODE={self.bot_mode.value} is incompatible with PAPER_MODE=true"
                raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _enforce_bakeoff_paper_only(self) -> "Settings":
        """Bake-off is a research harness that must never run in an
        order-executing mode. READ_ONLY emits no intents (bake-off is
        inert) and PAPER is the intended mode — both are allowed; only
        the executing modes (LIVE / LIVE_DRY / CLOSE_ONLY) are rejected.
        (main.py's `bakeoff_active` gate additionally requires PAPER, so
        the harness only actually runs under PAPER.) Stronger than a
        preflight check — fails Settings construction."""
        if self.bakeoff_enabled and self.bot_mode in _EXECUTING_MODES:
            msg = (
                f"BAKEOFF_ENABLED=true is incompatible with executing "
                f"mode {self.bot_mode.value}; bake-off runs only in PAPER"
            )
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _enforce_exposure_consistency(self) -> "Settings":
        if self.max_total_exposure_usd < self.max_position_usd:
            msg = (
                "MAX_TOTAL_EXPOSURE_USD must be >= MAX_POSITION_USD "
                f"(got {self.max_total_exposure_usd} < {self.max_position_usd})"
            )
            raise ValueError(msg)
        if self.max_position_usd_hard < self.max_position_usd:
            msg = (
                "MAX_POSITION_USD_HARD must be >= MAX_POSITION_USD "
                f"(got hard={self.max_position_usd_hard} < "
                f"soft={self.max_position_usd})"
            )
            raise ValueError(msg)
        if self.max_total_exposure_usd < self.max_position_usd_hard:
            # Belt-and-braces: the hard cap × max_open_positions must
            # not blow past the total exposure ceiling.
            msg = (
                "MAX_TOTAL_EXPOSURE_USD must be >= MAX_POSITION_USD_HARD "
                f"(got {self.max_total_exposure_usd} < "
                f"{self.max_position_usd_hard})"
            )
            raise ValueError(msg)
        return self
