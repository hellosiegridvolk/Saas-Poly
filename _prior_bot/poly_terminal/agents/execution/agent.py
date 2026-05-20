"""Execution Agent — paper writer + LIVE_DRY signer + LIVE submitter.

Buy side: subscribes to EVT_INTENT_APPROVED. Behavior depends on the
current `mode` (resolved per-intent via `mode_getter`):

- PAPER     → writes paper_fills + opens position, emits ORDER_FILLED
              and POSITION_OPENED. No live API calls.
- LIVE_DRY  → mirrors PAPER (so the bot's downstream agents continue
              working) AND signs a real Polymarket order via
              py-clob-client without POSTing it. The signed order is
              persisted to live_orders so the audit trail proves the
              EIP-712 signing path works end-to-end with no money.
- LIVE      → Phase 2 (sign + submit). Currently logs and degrades to
              LIVE_DRY behavior so an accidental promotion can never
              place real orders before the implementation is verified.

Sell side: subscribes to EVT_SELL_INTENT (from ExitAgent), writes a
closing paper_fills row, sets closed_ts/exit_price/realized_pnl/outcome
on the position, emits EVT_POSITION_CLOSED.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_INTENT_APPROVED,
    EVT_ORDER_FILLED,
    EVT_POSITION_CLOSED,
    EVT_POSITION_OPENED,
    EVT_SELL_INTENT,
)
from poly_terminal.data.clob.live_orders import LiveOrderClient
from poly_terminal.persistence.repositories.fills import (
    FillsRepo,
    PaperFillRow,
    PositionRow,
    PositionsRepo,
)
from poly_terminal.persistence.repositories.live_orders import (
    LiveOrderRow,
    LiveOrdersRepo,
)
from poly_terminal.persistence.repositories.reconciliation_locks import (
    ReconciliationLockRepo,
)
from poly_terminal.shared.enums import BotMode

logger = logging.getLogger(__name__)


def _extract_polymarket_order_id(response: Any) -> str | None:
    """Polymarket POST /order responses use varying field names across
    versions: `orderID`, `orderId`, sometimes nested under `order`.
    Try the common shapes."""
    if not isinstance(response, dict):
        return None
    for key in ("orderID", "orderId", "order_id"):
        v = response.get(key)
        if v:
            return str(v)
    nested = response.get("order")
    if isinstance(nested, dict):
        for key in ("orderID", "orderId", "order_id", "id"):
            v = nested.get(key)
            if v:
                return str(v)
    return None


# ── Phase 32 P2 — on-chain inventory pre-check ───────────────────────
#
# Belt-and-braces against v54-style chain races. Even with
# SELL_LIVE_MIN_HOLD_S at 30s, Polymarket V2's settlement window has
# been observed >85s. Pre-fix the SELL fires before chain has credited
# inventory and gets rejected with `balance: 0`.
#
# The pre-check reads the on-chain CTF balance just before submission;
# if balance < expected, the SELL is deferred so the next exit-decision
# tick can re-attempt rather than burning a known-bad submission slot.
#
# Tolerance: Polymarket V2 occasionally reports 99.x% of the requested
# size due to wei-rounding on partial fills. Accept >=99% of expected
# to avoid false-positive defers from microscopic dust deltas.
_ONCHAIN_INVENTORY_TOLERANCE = 0.99


async def _onchain_inventory_ok(
    *,
    check: Callable[[str, float], Any] | None,
    token_id: str,
    expected_shares: float,
) -> tuple[bool, str]:
    """Run the optional on-chain inventory check. Returns (ok, reason).

    `check` shape: `async (token_id, expected_shares) -> bool`.
        True  → on-chain balance >= expected (proceed).
        False → defer SELL.
        Raise → defer SELL (treat unknown as unsafe).
    """
    if check is None:
        return True, "no_checker"
    try:
        result = await check(token_id, float(expected_shares))
    except Exception as exc:  # noqa: BLE001 — refuse to gate on unknown
        return False, f"check_raised: {exc}"
    if bool(result):
        return True, "balance_sufficient"
    return False, "balance_insufficient"


def make_onchain_inventory_check(
    *,
    ctf_reader: Any,
    funder_address: str,
    tolerance: float = _ONCHAIN_INVENTORY_TOLERANCE,
) -> Callable[[str, float], Any] | None:
    """Build an `onchain_inventory_check` callable from a CTFBalanceReader.

    Returns `None` when prerequisites are missing (no reader, no funder
    address). The agent treats `None` as "checker disabled" and falls
    back to legacy behavior — useful for tests / paper-only deploys
    that don't need the gate.

    The reader is called via `asyncio.to_thread` because
    `CTFBalanceReader` is synchronous (urllib-based, no aiohttp dep).
    """
    if ctf_reader is None or not funder_address:
        return None

    async def _check(token_id: str, expected_shares: float) -> bool:
        try:
            actual = await asyncio.to_thread(
                ctf_reader.shares_of, funder_address, token_id
            )
        except Exception as exc:
            logger.debug(
                "onchain inventory check raised — deferring SELL "
                "(token=%s expected=%.4f exc=%s)",
                token_id, expected_shares, exc,
            )
            return False
        threshold = float(expected_shares) * float(tolerance)
        ok = float(actual) >= threshold
        if not ok:
            logger.info(
                "onchain inventory check deferring SELL: "
                "token=%s actual=%.4f threshold=%.4f (%.0f%% of expected=%.4f)",
                token_id, float(actual), threshold,
                tolerance * 100, float(expected_shares),
            )
        return ok

    return _check


def _extract_buy_fill(
    response: Any, fallback_price: float
) -> tuple[float, float] | None:
    """Parse a V2 POST /order response for an actual matched fill.

    Returns (filled_shares, avg_fill_price) on success, or None when
    the response indicates no match. FAK no-match raises an exception
    upstream, so reaching this code with a successful response means
    at least a partial fill — but the SDK's response shape varies
    across builds, so we look at multiple field names and fall back
    to inferring from `makingAmount` / `takingAmount` when present.

    Conservative: when fields are missing we return None rather than
    fabricate a fill, leaving the position un-inserted. The audit row
    is still in live_orders for later reconciliation.
    """
    if not isinstance(response, dict):
        return None
    # Failure shapes
    if response.get("success") is False:
        return None
    status = str(response.get("status", "")).lower()
    if status in ("delayed", "rejected", "killed", "cancelled"):
        return None
    # Polymarket V2 returns matched amounts as DECIMAL strings already
    # in human units (verified against live LIVE canary 2026-05-02:
    # `"takingAmount": "5.161289"` is shares, `"makingAmount": "1.599999"`
    # is USD). Earlier code divided by 1e6 assuming 6dp integer base
    # units — that squashed every recorded fill to ~5e-6 shares and
    # $0 cost basis. Do NOT divide.
    # For BUY: makingAmount = USDC paid, takingAmount = shares received.
    making = response.get("makingAmount") or response.get("making_amount")
    taking = response.get("takingAmount") or response.get("taking_amount")
    try:
        if taking is not None:
            shares = float(taking)
            usd = float(making) if making is not None else 0.0
            if shares > 0:
                avg = (usd / shares) if usd > 0 else fallback_price
                return shares, avg
    except (ValueError, TypeError, ZeroDivisionError):
        pass
    return None


def _extract_sell_fill(
    response: Any, fallback_price: float
) -> tuple[float, float] | None:
    """Parse a V2 POST /order response for a matched SELL fill.

    Mirror of `_extract_buy_fill` with the making/taking mapping
    swapped. For a SELL on Polymarket V2:
      - makingAmount = shares OUT (what we sold)
      - takingAmount = USDC IN  (what we received)

    Verified against the 2026-05-06 LIVE canary on pos 22319:
      `"takingAmount": "13.86", "makingAmount": "33", "status": "matched"`
    → 33 shares sold for 13.86 USDC = $0.42/share avg fill.

    2026-05-06 PHASE 4 — without this, the SELL handler would call
    `mark_submitted` and walk away, leaving live_orders.filled_qty=0
    and positions.exit_price stuck at the limit price (instead of
    the actual better fill). Pos 22319 actually filled at $0.42 but
    the bot recorded $0.05 (limit), turning a +$11 profit into a
    -$1.05 phantom loss.
    """
    if not isinstance(response, dict):
        return None
    if response.get("success") is False:
        return None
    status = str(response.get("status", "")).lower()
    if status in ("delayed", "rejected", "killed", "cancelled"):
        return None
    making = response.get("makingAmount") or response.get("making_amount")
    taking = response.get("takingAmount") or response.get("taking_amount")
    try:
        if making is not None:
            shares = float(making)  # SELL: makingAmount = shares OUT
            usd = float(taking) if taking is not None else 0.0
            if shares > 0:
                avg = (usd / shares) if usd > 0 else fallback_price
                return shares, avg
    except (ValueError, TypeError, ZeroDivisionError):
        pass
    return None


class ExecutionAgent:
    # SELL escalation defaults — used when the initial FAK SELL is
    # killed by Polymarket because no buyer is at our limit. We retry
    # at progressively lower prices to actually close the on-chain
    # position. See _handle_live_sell for the loop.
    #
    # 2026-05-02 round-trip-economics retune:
    # OLD (5 × 5%) gave 23% worst-case slippage — turned every TP into
    # a net loss after 2 retries. NEW (3 × 2%) caps slippage at 6%, so
    # a 15% TP target nets ≥ +9% even in the worst-case escalator
    # path. Tradeoff: positions that don't sell within 3 retries fall
    # back to the bar-resolution sweep, which is the right behavior for
    # genuinely illiquid books — we'd rather wait than fire-sale.
    SELL_ESCALATION_MAX_ATTEMPTS = 3      # initial + 2 retries
    SELL_ESCALATION_UNDERCUT_PCT = 0.02   # 2% per step
    SELL_ESCALATION_WAIT_S = 2.0          # backoff between attempts
    # 2026-05-07 PHASE 15 — entry-relative floor.
    # Pre-Phase-15 the floor was a hardcoded $0.05. That was sensible
    # for normal-priced markets (a $0.50 entry stops escalation at
    # 90% loss) but stranded penny-priced positions: v27 (pos 22445)
    # entered at $0.01, market dropped to $0.001, the very first
    # SELL retry tried to undercut from $0.001 → 0.001 × 0.98 =
    # 0.00098, which < $0.05 floor → bail. SELL_FAILED, 255 shares
    # stranded on-chain.
    #
    # Phase 15: floor = max(SELL_ESCALATION_MIN_PRICE,
    #                       entry_price × SELL_ESCALATION_MIN_PRICE_RATIO)
    # The ABS floor stays as a Polymarket-tick safety backstop ($0.001
    # is the lowest valid tick on the exchange). The RATIO scales with
    # entry: a $0.50 entry yields floor=$0.05 (legacy behavior); a
    # $0.01 entry yields floor=$0.001 (penny markets clear).
    SELL_ESCALATION_MIN_PRICE = 0.001     # absolute Polymarket tick
                                           # floor — never go below
    SELL_ESCALATION_MIN_PRICE_RATIO = 0.10 # ≥10% of entry price

    # 2026-05-06 PHASE 5 — chain-settle race protection.
    # Polymarket's exchange contract checks on-chain CTF balance before
    # allowing a SELL. When ProfitTaker fires within ~1s of the BUY
    # filling on Polymarket's matching engine, the BUY hasn't yet been
    # mined into Polygon — so the balance check refuses the SELL with
    # "not enough balance / allowance: balance: 0, order amount: N".
    # Two-layer fix:
    #   A) MIN_HOLD: if BUY's submitted_at is < SELL_LIVE_MIN_HOLD_S
    #      ago, sleep the difference before signing the first SELL.
    #      Prevents the race in the FIRST place rather than handling
    #      it via retry only.
    #   B) RETRY: if a SELL POST returns the balance-zero error,
    #      retry at the SAME price (it's a settle race, not a market
    #      issue — undercutting would just leave money on the table).
    #      Exponential backoff (base × 2^attempt). Bounded by
    #      SELL_BALANCE_RETRY_MAX_ATTEMPTS so a wallet that genuinely
    #      has no shares doesn't loop forever.
    # 2026-05-09 PHASE 31 P1c — production default 30s, test default 3s.
    # The class constant is the FALLBACK; the constructor accepts an
    # explicit `min_hold_s` override that the agent uses at runtime.
    # main.py wires the production value (30.0) from settings; tests
    # that don't exercise the gate pass `min_hold_s=0.0` to skip the
    # real asyncio.sleep, and gate-specific tests pass the value they
    # want to assert against. The patient SELL helper observes the
    # SAME instance attribute, so parity is preserved (Phase 31 win).
    #
    # Why 30s in production: v54 pos 22499 hit balance:0 at elapsed≈85s,
    # suggesting Polymarket V2's settlement window is unpredictable;
    # 3s was too tight for the chain race to clear in many cases.
    # 30s sacrifices some EXIT_SL_ABS speed but reduces SELL_FAILED
    # from chain races dramatically (replay shows pos 22499 would have
    # closed at SL like 22500 with this bump — full v50-v55 cumulative
    # would have been -$0.35 instead of -$10.67).
    SELL_LIVE_MIN_HOLD_S = 3.0  # class default = legacy/test value
    PRODUCTION_MIN_HOLD_S = 30.0  # what main.py wires explicitly
    # 2026-05-09 PHASE 31 P1d — bumped 3 → 6.
    # v54 pos 22499 hit balance:0 at +85s post-BUY-fill; the prior
    # 3-attempt cap (delays 3/6/12 = ~21s) exhausted before the chain
    # settled. 6 attempts (delays 3/6/12/24/48/96 = ~189s = ~3min)
    # gives the chain ample time to mine and credit balance, with a
    # bounded ceiling so genuinely-out-of-shares positions don't loop
    # forever. Combined with the patient SELL min-hold parity (P1c)
    # this should eliminate the v54-style chain-race SELL_FAILED.
    SELL_BALANCE_RETRY_MAX_ATTEMPTS = 6
    SELL_BALANCE_RETRY_BASE_DELAY_S = 3.0

    # 2026-05-07 PHASE 17 — sub-V2-min partial-fill guard.
    # Polymarket V2 has a $1 minimum order size for both BUY and SELL.
    # When a FAK BUY signs for >$1 worth but the orderbook only
    # partial-fills a fraction below $1 in cost basis, opening the
    # position causes a downstream cascade:
    #   - profit_taker tracks it
    #   - first downtick fires EXIT_SL_ABS
    #   - SELL sign fails (V2 SDK refuses < $1)
    #   - position closed as SELL_FAILED with stuck on-chain inventory
    # v30 (pos 22449) hit this: signed for ~5sh × $0.52, only 0.82
    # filled = $0.43 cost. Phase 17 detects the sub-min cost basis
    # at fill-parse time and refuses to open the position. The
    # on-chain shares ride to bar resolution; Phase 16 redeemer
    # auto-clears them. The live_orders row stays at status='filled'
    # for audit (the BUY did happen on-chain) but no profit_taker
    # tracking, no SELL attempts.
    SUBMINIMAL_FILL_THRESHOLD_USD = 1.0

    # 2026-05-07 PHASE 18 — disambiguate Polymarket's two
    # "balance not enough" error formats:
    #
    #   A) `balance: 0, order amount: N`
    #      → genuine chain-settle race; BUY hasn't mined; retry helps.
    #   B) `balance: N, sum of matched orders: N, order amount: N`
    #      → a previous SELL is currently matching on Polymarket's
    #        engine and consuming our allowance. The "in flight"
    #        order WILL fill in seconds; retrying just collides.
    #
    # Pre-Phase-18, the bot's regex matched both as case A. v31
    # (pos 22450) hit case B: SELL fired at $0.6627, Polymarket's
    # engine matched it at $0.53 (book moved during the ~150s of
    # FAK escalation), but the response framed the match-in-flight
    # as a balance error. The bot retried at $0.6627 twice, saw
    # "balance: 0" (because the first match consumed inventory),
    # exhausted, and Phase 7 restated the position as SELL_FAILED.
    # On-chain truth: 5 shares sold at $0.53 (+$0.15 profit) — the
    # bot's DB just didn't know.
    #
    # Phase 18: when we see "sum of matched orders" in the error,
    # don't retry. Poll Data API /trades for our SELL fill, restate
    # the close_price to the actual on-chain price, return.
    SELL_MATCHING_IN_FLIGHT_FRAGMENT = "sum of matched orders"
    # How long to wait for /trades to surface the fill before
    # falling back to legacy retry. Polymarket's engine usually
    # flushes within 1-3s; 8s is generous.
    SELL_RECONCILE_TIMEOUT_S = 8.0
    SELL_RECONCILE_POLL_INTERVAL_S = 1.0

    def __init__(
        self,
        bus: EventBus,
        fills_repo: FillsRepo,
        positions_repo: PositionsRepo,
        live_orders_repo: LiveOrdersRepo | None = None,
        live_client: LiveOrderClient | None = None,
        mode_getter: Callable[[], BotMode] | None = None,
        # 2026-05-07 PHASE 18 — async callable that returns the
        # funder wallet's recent SELL trades. Signature:
        #   (limit: int) -> Awaitable[list[dict]]
        # Each dict shape (per data-api.polymarket.com /trades):
        #   {side, asset, size, price, timestamp, transactionHash, ...}
        # When None, Phase 18 reconciliation is disabled and the
        # SELL handler falls back to legacy balance-retry behavior
        # for both error formats (preserves pre-Phase-18 semantics
        # for tests / read-only deploys).
        trades_fetcher: Callable[[int], Any] | None = None,
        # 2026-05-08 PHASE 28 — patient SELL on EXIT_SL.
        # When the getter returns True AND the position has at least
        # `patient_min_time_to_close_s` until bar resolution, the SELL
        # flow first attempts a GTC at `patient_target` price (default
        # last_trade_price), polls for fill, then cancels and falls
        # through to the legacy FAK escalation if not filled.
        # Default OFF — opt-in via env (SL_PATIENT_MODE=true).
        # Mirror of the mode_getter pattern so the operator can flip
        # behavior without restarting (e.g. via a runtime override).
        patient_mode_getter: Callable[[], bool] | None = None,
        patient_wait_s: int = 30,
        patient_target: str = "last_trade",
        patient_min_time_to_close_s: int = 600,
        # 2026-05-09 PHASE 31 — reconciliation lock repo.
        # When a SELL fails terminally (escalation exhausted, sign-fail
        # with on-chain BUY confirmed), set a quarantine lock on the
        # token so the position_importer skips re-importing leftover
        # on-chain shares as a "new" position. Closes the v50r2/v51/v52
        # phantom-double chain. Optional — None disables the wiring
        # (e.g. for tests / paper-only deploys).
        reconciliation_lock_repo: "ReconciliationLockRepo | None" = None,
        # 2026-05-09 PHASE 31 P1c — chain-settlement min-hold override.
        # When None, falls back to the class constant SELL_LIVE_MIN_HOLD_S.
        # Production wires PRODUCTION_MIN_HOLD_S (30.0); tests can pass
        # 0.0 to skip the real asyncio.sleep without mocking.
        min_hold_s: float | None = None,
        # 2026-05-09 PHASE 32 P2 — on-chain inventory pre-check.
        # Optional async callable `(token_id, expected_shares) -> bool`.
        # Returning False defers the SELL so the next exit-decision tick
        # can re-attempt rather than burning a known-bad submission slot.
        # When None (default), legacy behavior is preserved exactly.
        # Production wires `make_onchain_inventory_check(...)`; tests
        # default to None unless explicitly testing the defer path.
        onchain_inventory_check: Callable[[str, float], Any] | None = None,
    ) -> None:
        self._bus = bus
        self._fills = fills_repo
        self._positions = positions_repo
        self._live_repo = live_orders_repo
        self._trades_fetcher = trades_fetcher
        self._live_client = live_client
        self._mode_getter = mode_getter or (lambda: BotMode.PAPER)
        self._patient_mode_getter = patient_mode_getter or (lambda: False)
        self._patient_wait_s = patient_wait_s
        self._patient_target = patient_target
        self._patient_min_time_to_close_s = patient_min_time_to_close_s
        self._reconciliation_lock_repo = reconciliation_lock_repo
        # Phase 31 P1c — instance min-hold (production 30s, test 0s).
        self._min_hold_s = (
            float(min_hold_s) if min_hold_s is not None
            else float(self.SELL_LIVE_MIN_HOLD_S)
        )
        # Phase 32 P2 — on-chain inventory pre-check (optional).
        self._onchain_inventory_check = onchain_inventory_check
        self._started = False
        self._stats = {
            "fills_written": 0,
            "errors": 0,
            "positions_closed": 0,
            "close_misses": 0,
            "live_orders_signed": 0,
            "live_orders_submitted": 0,
            "live_order_errors": 0,
            "sell_escalation_retries": 0,
            "sell_escalation_filled": 0,
            "sell_escalation_exhausted": 0,
            # Phase 28 (2026-05-08) — patient-SELL counters.
            "patient_sell_attempted": 0,    # GTC submit succeeded
            "patient_sell_filled": 0,       # filled within wait
            "patient_sell_timeout": 0,      # no fill, fell back to FAK
            "patient_sell_skipped": 0,      # disabled or near-resolution
            "patient_sell_errors": 0,       # SDK exception on submit/poll
            # Phase 32 P2 (2026-05-09) — on-chain inventory pre-check.
            "sell_deferred_no_onchain_inventory": 0,
        }

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_INTENT_APPROVED, self._on_approved)
        self._bus.subscribe(EVT_SELL_INTENT, self._on_sell_intent)
        self._started = True

    async def _on_approved(self, _e: str, payload: Any) -> None:
        intent = payload if isinstance(payload, BuyIntent) else None
        if intent is None:
            self._stats["errors"] += 1
            return
        if intent.limit_price <= 0 or intent.size_usd <= 0:
            self._stats["errors"] += 1
            return
        signal_at = int(intent.created_at) if intent.created_at else int(time.time())
        filled_at = int(time.time())
        if filled_at < signal_at:
            filled_at = signal_at  # clock skew safety
        shares = float(intent.size_usd / intent.limit_price)

        try:
            mode = self._mode_getter()
        except Exception:
            mode = BotMode.PAPER

        # LIVE mode: defer ALL bookkeeping to actual on-chain fill
        # confirmation. Until 2026-05-02 the optimistic insert here
        # created 3000+ phantom positions when BUYs failed (FAK no-
        # match, $0.99 maker shrink, etc.) — ProfitTaker then "managed"
        # those phantoms, fired SELLs against zero inventory, and the
        # `positions` table accumulated paper PnL detached from real
        # money flow. _handle_live_buy now inserts the position only
        # after the V2 SDK returns a matched fill response.
        if mode is BotMode.LIVE:
            await self._handle_live_buy(intent, shares, filled_at)
            return

        # PAPER and LIVE_DRY keep the optimistic insert: PAPER has no
        # real money so the paper book IS the truth; LIVE_DRY is a
        # signature-only shadow used to validate the EIP-712 path
        # without posting, so paper bookkeeping continues to project
        # PnL alongside.
        try:
            await self._fills.insert_paper_fill(
                PaperFillRow(
                    intent_id=intent.intent_id,
                    strategy=intent.strategy,
                    market_id=intent.market_id,
                    token_id=intent.token_id,
                    side=intent.side.value,
                    qty=shares,
                    signal_price=float(intent.limit_price),
                    fill_price=float(intent.limit_price),
                    signal_at=signal_at,
                    filled_at=filled_at,
                )
            )
        except Exception:
            logger.exception("paper fill insert failed for intent %s", intent.intent_id)
            self._stats["errors"] += 1
            return

        position_id = await self._positions.open_position(
            PositionRow(
                market_id=intent.market_id,
                token_id=intent.token_id,
                side=intent.side.value,
                entry_price=float(intent.limit_price),
                shares=shares,
                cost_basis_usd=float(intent.size_usd),
                entry_intent_id=intent.intent_id,
                entry_ts=filled_at,
                end_date_iso=intent.end_date_iso,
                # Bake-off: intent.strategy IS the unique lane name when
                # active; in normal PAPER it is the plain family. Both
                # columns get it; scoreboard decomposes lane -> family.
                strategy=intent.strategy,
                lane_id=intent.strategy,
                source_wallet=intent.source_wallet,
            )
        )

        self._stats["fills_written"] += 1

        # LIVE_DRY shadow signing — no POST, no fill expected.
        await self._handle_live_buy(intent, shares, filled_at)

        await self._bus.publish(
            EVT_ORDER_FILLED,
            {
                "intent_id": intent.intent_id,
                "fill_price": float(intent.limit_price),
                "filled_size": shares,
                "filled_at": filled_at,
                "paper": True,
            },
        )
        await self._bus.publish(
            EVT_POSITION_OPENED,
            {
                "position_id": position_id,
                # 2026-05-06 — intent_id is REQUIRED by CanaryController
                # to look up the live_orders row and confirm a real LIVE
                # fill. Without it, the canary controller can never
                # flip mode from LIVE → CLOSE_ONLY after the first fill,
                # and the bot opens repeated real-money BUYs (production
                # bug, May 6: 4 LIVE BUYs before manual pause).
                "intent_id": intent.intent_id,
                "token_id": intent.token_id,
                "market_id": intent.market_id,
                "strategy": intent.strategy,
                "entry_price": str(intent.limit_price),
                "shares": str(Decimal(shares)),
                "cost_basis_usd": str(intent.size_usd),
                "entry_ts": float(filled_at),
                "exit_config": intent.exit_config,
                # Pass through so ExitAgent's bar-resolution watcher can
                # force-exit positions whose underlying bar has settled.
                "end_date_iso": intent.end_date_iso,
            },
        )

    # ── BUY re-quote tunables (used in LIVE mode before submit) ─────
    # Cap how far we'll chase the market above the strategy's signal
    # price. The followed-wallet's price often moves between the time
    # they execute and our signal arrives; bidding their stale price
    # via FAK was producing ~80% no-match. We re-quote against the
    # current best ASK and bid right at it, but never more than
    # BUY_MAX_SLIP above the strategy's intent.
    BUY_MAX_SLIP_PCT = 0.10        # never bid more than 10% over intent
    BUY_REQUOTE_BUFFER_PCT = 0.005 # 0.5% over best_ask to clear the spread

    # 2026-05-08 PHASE 19 — symmetric SELL re-quote against fresh
    # best_bid. Mirror of the BUY re-quote logic above. Without it,
    # `_handle_live_sell` submits at profit_taker's `price_hint`,
    # which comes from tick_poller's last poll (every 5s). Between
    # poll and SELL submission the orderbook can shift — on thin
    # 5-min binaries near expiry, the bid level vanishes in 1-2s.
    # v32 (pos 22451) hit this: tick_poller said best_bid=$0.07, but
    # by submission time the actual best_bid was below $0.0672 (the
    # last escalation attempt) → all 3 FAK attempts no_match → 62
    # shares stranded on-chain. Phase 19 re-queries best_bid fresh
    # at submit time and crosses it by SELL_REQUOTE_BUFFER_PCT.
    SELL_MAX_SLIP_PCT = 0.10        # never SELL more than 10% under intent
    SELL_REQUOTE_BUFFER_PCT = 0.005 # 0.5% under best_bid to clear the spread

    async def _handle_live_buy(
        self, intent: BuyIntent, shares: float, signed_at: int
    ) -> None:
        """LIVE_DRY: sign a real Polymarket order without POSTing it.
        LIVE: re-quote against the orderbook, sign + POST, and insert
        the position only on confirmed fill.
        """
        try:
            mode = self._mode_getter()
        except Exception:
            self._stats["live_order_errors"] += 1
            return
        if mode not in (BotMode.LIVE_DRY, BotMode.LIVE):
            return
        if self._live_repo is None or self._live_client is None:
            # Mode says LIVE_* but the bot was constructed without the
            # live plumbing. Loud log so the operator knows their boot
            # config is incoherent — but don't crash the trading loop.
            logger.warning(
                "execution: mode=%s but no live_client/live_repo wired; "
                "live order skipped for intent %s",
                mode.value, intent.intent_id,
            )
            return

        # ── Re-quote against orderbook (LIVE only; LIVE_DRY skips
        # this since there's no submission to clamp). ──────────────
        effective_price = float(intent.limit_price)
        if mode is BotMode.LIVE and intent.side.value == "BUY":
            best_ask = await asyncio.to_thread(
                self._live_client.get_best_ask, intent.token_id
            )
            if best_ask is None or best_ask <= 0:
                # No book or fetch failed — submit at original limit
                # and let FAK kill it if there's no contra. Logged so
                # we can monitor frequency.
                logger.info(
                    "live_orders: no best_ask for token %s — using "
                    "original limit %.4f",
                    intent.token_id, effective_price,
                )
            else:
                slip_cap = float(intent.limit_price) * (1.0 + self.BUY_MAX_SLIP_PCT)
                target = best_ask * (1.0 + self.BUY_REQUOTE_BUFFER_PCT)
                if target > slip_cap:
                    logger.info(
                        "live_orders: market moved beyond slip cap for "
                        "intent %s (intent=%.4f, best_ask=%.4f, "
                        "slip_cap=%.4f) — skipping",
                        intent.intent_id, intent.limit_price, best_ask,
                        slip_cap,
                    )
                    self._stats["live_buy_skipped_slip"] = (
                        self._stats.get("live_buy_skipped_slip", 0) + 1
                    )
                    return
                effective_price = max(best_ask, min(target, slip_cap))
                # Recompute shares against the re-quoted price so the
                # dollar budget stays the same; _build_signed will
                # upsize if needed to clear V2 maker-min.
                shares = float(intent.size_usd) / effective_price

        client_order_id = f"poly-v3-{intent.intent_id}"
        # Step 1 — sign. Identical for both LIVE_DRY and LIVE so the
        # signing path is exercised the same way in both. The audit row
        # lands at status='signed' first; LIVE then advances it.
        try:
            sign_result = await self._live_client.sign_only(
                token_id=intent.token_id,
                price=effective_price,
                size=shares,
                side=intent.side.value,
            )
        except Exception:
            logger.exception(
                "live_orders: sign failed for intent %s (mode=%s)",
                intent.intent_id, mode.value,
            )
            self._stats["live_order_errors"] += 1
            return

        try:
            await self._live_repo.insert(
                LiveOrderRow(
                    intent_id=intent.intent_id,
                    strategy=intent.strategy,
                    market_id=intent.market_id,
                    token_id=intent.token_id,
                    side=intent.side.value,
                    limit_price=effective_price,
                    size_usd=float(intent.size_usd),
                    shares=shares,
                    # FAK (Fill-And-Kill): execute whatever can fill at
                    # limit and cancel the rest. Prevents BUYs from
                    # accumulating as resting orders that hold pUSD
                    # allowance without ever filling.
                    order_type="FAK",
                    mode=mode.value,
                    client_order_id=client_order_id,
                    signed_order_json=sign_result.signed_order_json,
                    signed_at=signed_at,
                    status="signed",
                )
            )
        except Exception:
            logger.exception(
                "live_orders: persist failed for intent %s", intent.intent_id
            )
            self._stats["live_order_errors"] += 1
            return
        self._stats["live_orders_signed"] += 1

        # Step 2 — submit, LIVE only.
        if mode is not BotMode.LIVE:
            return
        try:
            submit_result = await self._live_client.sign_and_submit(
                token_id=intent.token_id,
                price=effective_price,
                size=shares,
                side=intent.side.value,
                # See FAK rationale on the audit-row insert above.
                order_type="FAK",
            )
        except Exception as exc:
            logger.exception(
                "live_orders: POST failed for intent %s", intent.intent_id
            )
            self._stats["live_order_errors"] += 1
            try:
                await self._live_repo.mark_rejected(
                    client_order_id=client_order_id,
                    response_json=json.dumps({"error": str(exc)}),
                    ts=int(time.time()),
                )
            except Exception:
                logger.exception(
                    "live_orders: mark_rejected failed for %s", client_order_id
                )
            return

        try:
            await self._live_repo.mark_submitted(
                client_order_id=client_order_id,
                response_json=json.dumps(submit_result.response or {}),
                submitted_at=int(time.time()),
            )
        except Exception:
            logger.exception(
                "live_orders: mark_submitted failed for %s", client_order_id
            )
            self._stats["live_order_errors"] += 1
            return
        # Capture Polymarket's order hash so the LiveFillReconciler can
        # map incoming TRADE / ORDER WS events back to this row.
        polymarket_order_id = _extract_polymarket_order_id(
            submit_result.response
        )
        if polymarket_order_id:
            try:
                await self._live_repo.set_polymarket_order_id(
                    client_order_id=client_order_id,
                    polymarket_order_id=polymarket_order_id,
                    ts=int(time.time()),
                )
            except Exception:
                logger.exception(
                    "live_orders: set_polymarket_order_id failed for %s",
                    client_order_id,
                )
        self._stats["live_orders_submitted"] += 1

        # Step 3 — only insert the position row if the response shows
        # an actual matched fill. FAK no-match raises an exception
        # (handled above); a successful response without parseable
        # fill data leaves the audit row in 'submitted' state for the
        # LiveFillReconciler to settle later, but we do NOT open a
        # position the bot would try to manage. This is the fix for
        # the 3000+ phantom positions accumulated through 2026-05-02.
        fill = _extract_buy_fill(submit_result.response, effective_price)
        if fill is None:
            logger.info(
                "live_orders: BUY POST succeeded for intent %s but no "
                "matched fill in response — deferring position open",
                intent.intent_id,
            )
            return
        filled_shares, avg_fill_price = fill
        cost_basis = filled_shares * avg_fill_price

        # 2026-05-07 PHASE 17 — sub-V2-min partial-fill guard. See the
        # SUBMINIMAL_FILL_THRESHOLD_USD docstring above for the v30
        # incident that prompted this gate. We DO still update the
        # live_orders row to filled (the BUY landed on-chain) so the
        # audit trail is honest; we just refuse to track a position
        # that can't be SELL'd. On-chain inventory is auto-redeemed
        # by Phase 16 at bar resolution.
        is_submin = cost_basis < self.SUBMINIMAL_FILL_THRESHOLD_USD

        # 2026-05-05 canary forensic fix: when the SDK match response
        # confirms a fill, eagerly update live_orders to status='filled'
        # + filled_qty. The LiveFillReconciler used to be the sole path
        # for this update (via UserWebSocket EVT_ORDER_FILLED), but the
        # canary on 2026-05-05 exposed that Polymarket's WS is bursty
        # and may not deliver fill events reliably — leaving rows stuck
        # at status='submitted' / filled_qty=0 even after real on-chain
        # fills. Without this update the CanaryControllerAgent (which
        # gates on filled_qty>0) silently never fires. Idempotent —
        # if the WS path also fires, record_fill aggregates by
        # polymarket_order_id and the canonical state is reached.
        if polymarket_order_id:
            try:
                await self._live_repo.record_fill(
                    polymarket_order_id=polymarket_order_id,
                    fill_qty=filled_shares,
                    fill_price=avg_fill_price,
                    ts=int(time.time()),
                    terminal=True,
                )
            except Exception:
                logger.exception(
                    "live_orders: eager record_fill failed for %s — "
                    "LiveFillReconciler will retry via UserWS",
                    polymarket_order_id,
                )
                self._stats["live_order_errors"] += 1

        # 2026-05-07 PHASE 17 — sub-V2-min reject. Skip position open
        # AND EVT_POSITION_OPENED. The live_orders row above is the
        # on-chain audit; Phase 16 redeemer will clear inventory.
        # Note: canary_controller will not flip on this path (no
        # EVT_POSITION_OPENED) — that's intentional. A sub-min fill
        # means the orderbook was thin; the next signal can retry
        # and may get a clean fill that does flip canary mode.
        if is_submin:
            self._stats["submin_fills_rejected"] = (
                self._stats.get("submin_fills_rejected", 0) + 1
            )
            logger.warning(
                "live_orders: SUB-MIN FILL %.4f sh × $%.4f = $%.4f < "
                "V2 floor $%.2f for intent %s — refusing to open "
                "position. On-chain shares (%.4f) will be cleared by "
                "the redeemer at bar resolution. (Phase 17 guard)",
                filled_shares, avg_fill_price, cost_basis,
                self.SUBMINIMAL_FILL_THRESHOLD_USD, intent.intent_id,
                filled_shares,
            )
            return

        try:
            position_id = await self._positions.open_position(
                PositionRow(
                    market_id=intent.market_id,
                    token_id=intent.token_id,
                    side=intent.side.value,
                    entry_price=avg_fill_price,
                    shares=filled_shares,
                    cost_basis_usd=cost_basis,
                    entry_intent_id=intent.intent_id,
                    entry_ts=int(time.time()),
                    end_date_iso=intent.end_date_iso,
                    source_wallet=intent.source_wallet,
                )
            )
        except Exception:
            logger.exception(
                "live_orders: open_position failed after BUY fill "
                "for intent %s", intent.intent_id,
            )
            self._stats["live_order_errors"] += 1
            return
        self._stats["live_orders_filled"] = (
            self._stats.get("live_orders_filled", 0) + 1
        )
        await self._bus.publish(
            EVT_POSITION_OPENED,
            {
                "position_id": position_id,
                # 2026-05-06 — see PAPER-path comment above.
                # Without this field, CanaryController never fires.
                "intent_id": intent.intent_id,
                "token_id": intent.token_id,
                "market_id": intent.market_id,
                "strategy": intent.strategy,
                "entry_price": str(avg_fill_price),
                "shares": str(Decimal(filled_shares)),
                "cost_basis_usd": str(cost_basis),
                "entry_ts": float(time.time()),
                "exit_config": intent.exit_config,
                "end_date_iso": intent.end_date_iso,
            },
        )

    async def _on_sell_intent(self, _e: str, payload: Any) -> None:
        """Handle EVT_SELL_INTENT from the ExitAgent.

        Writes a closing paper_fills row, calls
        PositionsRepo.close_position to set closed_ts/exit_price/
        realized_pnl/outcome, and emits EVT_POSITION_CLOSED.
        """
        if not isinstance(payload, dict):
            self._stats["errors"] += 1
            return
        try:
            position_id = int(payload["position_id"])
            shares = float(Decimal(str(payload["shares"])))
            price_hint = Decimal(str(payload["price_hint"]))
            reason = str(payload.get("reason", "EXIT_TIME"))
            strategy = str(payload.get("strategy", ""))
            token_id = str(payload.get("token_id", ""))
            # 2026-05-08 PHASE 28 — capture end_date_iso for the patient
            # SELL flow's time-to-close gate. Optional; falls back to
            # None which means "skip the time gate" (allow patient mode
            # without an end-date).
            end_date_iso = payload.get("end_date_iso")
        except (KeyError, TypeError, ValueError):
            self._stats["errors"] += 1
            return
        # 2026-05-07 PHASE 9 — recognize legitimate resolution-loss
        # closes. When a Polymarket bar resolves AGAINST our token,
        # the post-resolution price is exactly $0. ExitAgent fires
        # EVT_SELL_INTENT with reason=EXIT_TIME and price_hint=$0
        # (truth: token is worth $0). Pre-fix, the defensive guard
        # below treated this as invalid noise → position stayed
        # open forever → MAX_OPEN_POSITIONS cap blocked all new
        # intents (May 6 overnight pos 22337: 15h stall, 4035 intents
        # rejected at open_positions_cap_exceeded gate).
        #
        # Phase 9: only the (EXIT_TIME, price=$0) combination gets
        # the resolution-loss treatment — close at exit_price=0,
        # realized=-cost_basis, outcome=LIVE_RESOLVED_LOSS, skip the
        # live SELL (no point selling at $0). All other reasons at
        # price<=0 are still rejected as invalid (defensive: a TP/SL
        # at $0 would indicate a corrupted WS payload).
        is_resolution_loss = (
            price_hint <= 0
            and reason == "EXIT_TIME"
        )
        if price_hint <= 0 and not is_resolution_loss:
            self._stats["errors"] += 1
            return

        opened = await self._positions.fetch_open(position_id)
        if opened is None:
            # Already closed or unknown — count miss but don't crash.
            self._stats["close_misses"] += 1
            return

        if is_resolution_loss:
            # Close as LIVE_RESOLVED_LOSS, skip the live SELL path
            # entirely. The $0 close is the truth: bar resolved
            # against us, shares are worthless, no SELL fill exists
            # or is achievable.
            cost_basis = float(opened.get("cost_basis_usd", 0.0))
            try:
                snapshot = await self._positions.close_position(
                    position_id=position_id,
                    exit_price=0.0,
                    outcome="LIVE_RESOLVED_LOSS",
                    closed_ts=int(time.time()),
                )
            except Exception:
                logger.exception(
                    "execution: Phase 9 close_position failed for "
                    "position %s (resolution loss)", position_id,
                )
                self._stats["errors"] += 1
                return
            if snapshot is None:
                self._stats["close_misses"] += 1
                return
            self._stats["positions_closed"] += 1
            self._stats["resolution_loss_closes"] = (
                self._stats.get("resolution_loss_closes", 0) + 1
            )
            logger.info(
                "execution: Phase 9 resolution-loss close — "
                "position %s closed at $0 (bar resolved against us, "
                "cost_basis=$%.4f, outcome=LIVE_RESOLVED_LOSS)",
                position_id, cost_basis,
            )
            try:
                await self._bus.publish(
                    EVT_POSITION_CLOSED, dict(snapshot)
                )
            except Exception:
                logger.exception(
                    "execution: EVT_POSITION_CLOSED publish failed "
                    "for resolution-loss position %s", position_id,
                )
            return

        now_ts = int(time.time())
        market_id = str(opened.get("market_id", ""))
        # Use entry_ts as signal_at for the closing fill so the
        # CHECK constraint (filled_at >= signal_at) still holds.
        signal_at = int(opened.get("entry_ts", now_ts))
        if now_ts < signal_at:
            now_ts = signal_at

        # Map the EXIT_* enum value to a short outcome label that fits the
        # daily-check / acceptance-gate categorisation.
        outcome_map = {
            "EXIT_TP": "TP",
            "EXIT_TP_ABS": "TP",     # ProfitTakerAgent — same outcome label
            "EXIT_TP_TRAIL": "TP",   # trailing-profit lock
            "EXIT_SL": "SL",
            "EXIT_SL_ABS": "SL",     # ProfitTakerAgent loss side
            "EXIT_TIME": "TIME",
            "EXIT_WHALE_OUT": "WHALE_OUT",
            "HOLD": "TIME",  # safety
        }
        outcome = outcome_map.get(reason, "TIME")

        # Closing paper_fill row.
        try:
            await self._fills.insert_paper_fill(
                PaperFillRow(
                    intent_id=str(opened.get("entry_intent_id", "")) + ":exit",
                    strategy=strategy or "copy_trade",
                    market_id=market_id,
                    token_id=token_id or str(opened.get("token_id", "")),
                    side="SELL",
                    qty=shares,
                    signal_price=float(price_hint),
                    fill_price=float(price_hint),
                    signal_at=signal_at,
                    filled_at=now_ts,
                )
            )
        except Exception:
            logger.exception(
                "execution: closing fill insert failed for position %s",
                position_id,
            )
            self._stats["errors"] += 1
            return

        snapshot = await self._positions.close_position(
            position_id=position_id,
            exit_price=float(price_hint),
            outcome=outcome,
            closed_ts=now_ts,
        )
        if snapshot is None:
            self._stats["close_misses"] += 1
            return

        self._stats["positions_closed"] += 1
        self._stats["fills_written"] += 1

        # Sell-side live order shadow: in LIVE_DRY/LIVE we ALSO sign
        # (and in LIVE submit) a real Polymarket SELL. Failures are
        # logged but never block the paper-mode close — PnL projection
        # continues even if the live exit can't be placed.
        await self._handle_live_sell(
            position_id=position_id,
            entry_intent_id=str(opened.get("entry_intent_id", "")),
            strategy=strategy or "copy_trade",
            market_id=market_id,
            token_id=token_id or str(opened.get("token_id", "")),
            shares=shares,
            price_hint=float(price_hint),
            # 2026-05-07 PHASE 15 — needed for the entry-relative
            # SELL escalation floor. Without it, penny-priced
            # positions (e.g., entry $0.01) can't escalate below the
            # legacy $0.05 hard floor and SELL_FAIL with stuck
            # inventory on-chain.
            entry_price=float(opened.get("entry_price", 0)),
            now_ts=now_ts,
            end_date_iso=end_date_iso,
        )

        await self._bus.publish(
            EVT_POSITION_CLOSED,
            {
                "position_id": position_id,
                "token_id": token_id or str(opened.get("token_id", "")),
                "market_id": market_id,
                "strategy": strategy,
                "outcome": outcome,
                "reason": reason,
                "exit_price": float(price_hint),
                "realized_pnl": float(snapshot.get("realized_pnl", 0)),
                "closed_ts": now_ts,
            },
        )

    async def _reconcile_sell_via_trades(
        self,
        *,
        token_id: str,
        target_size: float,
        since_ts: int,
    ) -> dict[str, Any] | None:
        """2026-05-07 PHASE 18 — poll Data API /trades for our SELL
        fill after a matching-in-flight error.

        Polls `_trades_fetcher` on `SELL_RECONCILE_POLL_INTERVAL_S`
        until either:
          - a SELL trade matching `token_id` with timestamp >=
            `since_ts` and size within 0.001 of `target_size` is
            found → return the trade dict
          - `SELL_RECONCILE_TIMEOUT_S` elapses → return None
            (caller falls back to balance-retry path)

        Tolerance on size (0.001) handles floating-point quirks in
        the API's size encoding without admitting partial fills as
        full closes — partial fills are still a Phase 19 problem.
        """
        if self._trades_fetcher is None:
            return None
        deadline = time.time() + self.SELL_RECONCILE_TIMEOUT_S
        while time.time() < deadline:
            try:
                trades = await self._trades_fetcher(20)
            except Exception:
                logger.exception(
                    "phase18: trades fetch raised — aborting reconcile"
                )
                return None
            for t in trades or []:
                if str(t.get("side", "")).upper() != "SELL":
                    continue
                if str(t.get("asset", "")) != token_id:
                    continue
                try:
                    ts = int(t.get("timestamp", 0))
                    sz = float(t.get("size", 0))
                except (TypeError, ValueError):
                    continue
                if ts < since_ts:
                    continue
                if abs(sz - target_size) > 0.001:
                    # Size doesn't match — likely a partial fill or
                    # a different SELL. Skip; Phase 19 will handle
                    # partials explicitly.
                    continue
                return t
            await asyncio.sleep(self.SELL_RECONCILE_POLL_INTERVAL_S)
        return None

    async def _try_patient_sell(
        self,
        *,
        position_id: int,
        entry_intent_id: str,
        strategy: str,
        market_id: str,
        token_id: str,
        shares: float,
        price_hint: float,
        end_date_iso: str | None,
    ) -> bool:
        """Phase 28 — opt-in patient SELL on EXIT_SL.

        Submits a GTC SELL at a "scalp" price (typically the last
        traded price), polls the order's status for up to
        `_patient_wait_s` seconds, and cancels + falls through to
        legacy FAK if it doesn't fill in time.

        Returns True if the GTC filled within the wait window
        (caller should NOT run FAK). False otherwise — caller falls
        through to existing escalation logic. All counter updates
        and exception handling are internal; the caller only sees
        the boolean.
        """
        if self._live_client is None:
            return False
        # 1) Time-to-close gate — refuse patient mode on near-resolution
        # bars where the wait would consume too much of the position's
        # remaining life.
        if (
            self._patient_min_time_to_close_s > 0
            and end_date_iso
        ):
            try:
                from datetime import datetime as _dt
                eod = _dt.fromisoformat(
                    str(end_date_iso).replace("Z", "+00:00")
                )
                secs_left = int(eod.timestamp()) - int(time.time())
                if secs_left < self._patient_min_time_to_close_s:
                    self._stats["patient_sell_skipped"] = (
                        self._stats.get("patient_sell_skipped", 0) + 1
                    )
                    logger.info(
                        "live_orders: patient SELL skipped for pos %s "
                        "— %ds to close < floor %ds",
                        position_id, secs_left,
                        self._patient_min_time_to_close_s,
                    )
                    return False
            except (ValueError, TypeError):
                pass  # malformed end_date — treat as time-OK
        # 2) Compute scalp price.
        scalp_price = await self._patient_scalp_price(token_id, price_hint)
        if scalp_price is None or scalp_price <= 0:
            self._stats["patient_sell_skipped"] = (
                self._stats.get("patient_sell_skipped", 0) + 1
            )
            return False
        # 2.5) 2026-05-09 — chain-settlement min-hold gate.
        # Mirrors `_handle_live_sell`'s Phase 5 PART A logic. v54 pos
        # 22499 lost on chain-settlement race: patient SELL fired
        # before Polymarket V2's balance engine had credited the
        # BUY's shares, causing balance:0 rejections on FAK fallback.
        # Wait at least SELL_LIVE_MIN_HOLD_S since the BUY's
        # submitted_at before submitting the GTC. One-shot wait per
        # SELL — if the gap is already past, no delay.
        if entry_intent_id and self._live_repo is not None:
            buy_coid = f"poly-v3-{entry_intent_id}"
            try:
                buy_row = await self._live_repo.fetch_by_client_id(buy_coid)
            except Exception:
                buy_row = None
            if buy_row is not None:
                buy_submitted_at = buy_row.get("submitted_at")
                if buy_submitted_at:
                    elapsed_s = time.time() - float(buy_submitted_at)
                    needed = self._min_hold_s - elapsed_s
                    if needed > 0:
                        logger.info(
                            "live_orders: patient SELL min-hold wait "
                            "%.2fs for position %s (BUY filled %.2fs ago)",
                            needed, position_id, elapsed_s,
                        )
                        self._stats["patient_sell_min_hold_waits"] = (
                            self._stats.get(
                                "patient_sell_min_hold_waits", 0
                            ) + 1
                        )
                        await asyncio.sleep(needed)
        # 2026-05-09 PHASE 32 P2 — on-chain inventory pre-check parity
        # with the legacy FAK path. If the chain hasn't credited the
        # BUY yet, defer this patient attempt — caller falls through
        # to FAK which will run the same check after its own min-hold.
        ok, reason = await _onchain_inventory_ok(
            check=self._onchain_inventory_check,
            token_id=token_id,
            expected_shares=float(shares),
        )
        if not ok:
            logger.info(
                "live_orders: deferring patient SELL for position %s — "
                "on-chain inventory pre-check failed (%s)",
                position_id, reason,
            )
            self._stats["sell_deferred_no_onchain_inventory"] = (
                self._stats.get(
                    "sell_deferred_no_onchain_inventory", 0
                ) + 1
            )
            return False
        # 3) Submit GTC SELL.
        try:
            submit_result = await self._live_client.sign_and_submit(
                token_id=token_id,
                price=float(scalp_price),
                size=float(shares),
                side="SELL",
                order_type="GTC",
            )
        except Exception:
            logger.exception(
                "live_orders: patient SELL GTC submit failed for "
                "position %s — falling through to FAK", position_id,
            )
            self._stats["patient_sell_errors"] = (
                self._stats.get("patient_sell_errors", 0) + 1
            )
            return False
        self._stats["patient_sell_attempted"] = (
            self._stats.get("patient_sell_attempted", 0) + 1
        )
        order_id = _extract_polymarket_order_id(submit_result.response)
        # 2026-05-09 PHASE 30(a) — persist patient SELL audit row.
        # v48 found pos 22489's patient SELL filled at $0.05 but no
        # row appeared in live_orders. Without this row the user-
        # channel WS's EVT_ORDER_FILLED has nothing to join against
        # via record_fill, and forensic queries lose the closing leg
        # of every successful patient exit. Distinct ":patient_exit"
        # suffix so legacy FAK ":exit" rows aren't collided with.
        patient_client_order_id = (
            f"poly-v3-{entry_intent_id}:pos{position_id}:patient_exit"
            if entry_intent_id
            else f"poly-v3-pos:{position_id}:patient_exit"
        )
        now_ts = int(time.time())
        try:
            await self._live_repo.insert(
                LiveOrderRow(
                    intent_id=(
                        f"{entry_intent_id}:patient_exit"
                        if entry_intent_id
                        else f"pos:{position_id}:patient_exit"
                    ),
                    strategy=strategy,
                    market_id=market_id,
                    token_id=token_id,
                    side="SELL",
                    limit_price=float(scalp_price),
                    size_usd=float(scalp_price) * float(shares),
                    shares=float(shares),
                    order_type="GTC",
                    mode=self._mode_getter().value,
                    client_order_id=patient_client_order_id,
                    signed_order_json=submit_result.signed_order_json,
                    signed_at=now_ts,
                    submitted_at=now_ts,
                    status="submitted",
                    order_response_json=None,
                )
            )
            if order_id:
                await self._live_repo.set_polymarket_order_id(
                    client_order_id=patient_client_order_id,
                    polymarket_order_id=order_id,
                    ts=now_ts,
                )
        except Exception:
            # Persistence failure must not block the SELL — the GTC
            # is already on-chain. Log and continue; counters will
            # still reflect the submit, only the audit row is lost.
            logger.exception(
                "live_orders: patient SELL audit persist failed for "
                "position %s (GTC already submitted)", position_id,
            )
        # 4) Poll for fill.
        deadline = time.time() + self._patient_wait_s
        poll_interval = max(0.5, self._patient_wait_s / 10)
        filled = False
        while time.time() < deadline:
            try:
                status = await self._live_client.get_order_status(order_id or "")
            except Exception:
                status = None
            if status:
                state = str(status.get("status", "")).lower()
                try:
                    fq = float(status.get("filled_qty") or 0)
                except (TypeError, ValueError):
                    fq = 0.0
                if state in ("filled", "matched") or fq >= float(shares) - 1e-6:
                    filled = True
                    break
            await asyncio.sleep(poll_interval)
        if filled:
            self._stats["patient_sell_filled"] = (
                self._stats.get("patient_sell_filled", 0) + 1
            )
            logger.info(
                "live_orders: patient SELL FILLED for pos %s @ %.4f "
                "(scalp price; no FAK fallback)",
                position_id, float(scalp_price),
            )
            return True
        # 5) Timeout → cancel and fall through.
        self._stats["patient_sell_timeout"] = (
            self._stats.get("patient_sell_timeout", 0) + 1
        )
        cancel_succeeded = False
        if order_id:
            try:
                await self._live_client.cancel_order(order_id)
                cancel_succeeded = True
            except Exception:
                logger.exception(
                    "live_orders: patient cancel_order raised for %s "
                    "(non-fatal; FAK still runs)", order_id,
                )
        # 2026-05-09 PHASE 31 P1b — patient_exit terminal status.
        # Pre-fix the live_orders row stayed at 'submitted' forever
        # even after the GTC was cancelled, leaving an inaccurate
        # audit trail. Now advance to 'cancelled' (cancel succeeded)
        # or 'cancel_failed' (cancel raised) — operator can see the
        # real terminal state.
        if self._live_repo is not None and order_id:
            new_status = "cancelled" if cancel_succeeded else "cancel_failed"
            try:
                await self._live_repo.set_status(
                    polymarket_order_id=order_id,
                    status=new_status,
                    ts=int(time.time()),
                )
            except Exception:
                logger.exception(
                    "live_orders: patient_exit status update to '%s' "
                    "failed for order %s (non-fatal)",
                    new_status, order_id,
                )
        logger.info(
            "live_orders: patient SELL timed out after %ds for pos %s "
            "@ scalp %.4f — cancelling and falling through to FAK",
            self._patient_wait_s, position_id, float(scalp_price),
        )
        return False

    async def _patient_scalp_price(
        self, token_id: str, price_hint: float
    ) -> float | None:
        """Compute the patient-mode scalp price based on
        `_patient_target` selector."""
        if self._live_client is None:
            return None
        target = (self._patient_target or "last_trade").lower()
        try:
            if target == "last_trade":
                v = await asyncio.to_thread(
                    self._live_client.get_last_trade_price, token_id
                )
                return float(v) if v else None
            if target == "best_ask":
                v = await asyncio.to_thread(
                    self._live_client.get_best_ask, token_id
                )
                return float(v) if v else None
            if target == "midpoint":
                ask = await asyncio.to_thread(
                    self._live_client.get_best_ask, token_id
                )
                bid = await asyncio.to_thread(
                    self._live_client.get_best_bid, token_id
                )
                if ask and bid:
                    return (float(ask) + float(bid)) / 2
                return None
        except Exception:
            return None
        return None

    async def _handle_live_sell(
        self,
        *,
        position_id: int,
        entry_intent_id: str,
        strategy: str,
        market_id: str,
        token_id: str,
        shares: float,
        price_hint: float,
        now_ts: int,
        entry_price: float = 0.0,
        end_date_iso: str | None = None,
    ) -> None:
        """Sign + (in LIVE) submit the live SELL counterpart of an
        ExitAgent decision. Mirrors `_handle_live_buy` but flips side.

        client_order_id is suffixed `:exit` so the audit row is
        unambiguously a closing order — and matches the paper_fills
        intent_id convention so cross-table forensics line up.
        """
        try:
            mode = self._mode_getter()
        except Exception:
            self._stats["live_order_errors"] += 1
            return
        # 2026-05-05: CLOSE_ONLY signs + submits real SELLs (the whole
        # point of the mode is to wind down inventory on-chain).
        # LIVE_DRY signs only. PAPER/READ_ONLY skip entirely.
        if mode not in (BotMode.LIVE_DRY, BotMode.LIVE, BotMode.CLOSE_ONLY):
            return
        if self._live_repo is None or self._live_client is None:
            logger.warning(
                "execution: mode=%s but no live_client/live_repo wired; "
                "live SELL skipped for position %s",
                mode.value, position_id,
            )
            return
        if shares <= 0 or price_hint <= 0:
            self._stats["live_order_errors"] += 1
            return

        # Inventory gate: only place a live SELL when the matching BUY
        # actually put shares on-chain. Positions opened in PAPER (or
        # whose LIVE BUY was rejected) have no on-chain inventory, so
        # the SELL would always fail with `balance: 0` and just spam
        # the audit table. We key off the BUY's client_order_id
        # (`poly-v3-{intent_id}`) and require *positive filled_qty* —
        # a 'submitted' BUY may still be resting unfilled on the
        # orderbook, in which case the funder owns nothing yet.
        # Enforced in LIVE and CLOSE_ONLY — in LIVE_DRY we still want
        # to exercise the sign path even for paper positions.
        if mode in (BotMode.LIVE, BotMode.CLOSE_ONLY):
            if not entry_intent_id:
                logger.info(
                    "live_orders: skipping live SELL for position %s — "
                    "no entry_intent_id to verify on-chain inventory",
                    position_id,
                )
                self._stats["live_sell_skipped_no_inventory"] = (
                    self._stats.get("live_sell_skipped_no_inventory", 0) + 1
                )
                return
            buy_coid = f"poly-v3-{entry_intent_id}"
            buy_row = await self._live_repo.fetch_by_client_id(buy_coid)
            buy_status = (buy_row or {}).get("status")
            buy_filled_qty = float((buy_row or {}).get("filled_qty") or 0)
            if buy_status not in ("filled", "partial") or buy_filled_qty <= 0:
                logger.info(
                    "live_orders: skipping live SELL for position %s "
                    "(intent=%s) — BUY has no on-chain inventory "
                    "(buy_status=%s, filled_qty=%s)",
                    position_id, entry_intent_id, buy_status, buy_filled_qty,
                )
                self._stats["live_sell_skipped_no_inventory"] = (
                    self._stats.get("live_sell_skipped_no_inventory", 0) + 1
                )
                return

        # 2026-05-08 PHASE 28 — patient SELL on EXIT_SL.
        # When opt-in flag is set (env SL_PATIENT_MODE=true) AND the
        # market has enough time before resolution, try a GTC SELL at
        # a "scalp" price first, poll for fill, cancel + fall through
        # if it doesn't match within the wait window. Captures the v44
        # operator-observed pattern: place a higher SELL and wait for
        # someone to take it instead of dumping at the bid (FAK).
        try:
            # 2026-05-08 PHASE 28 fix: patient mode must also engage in
            # CLOSE_ONLY because the canary controller flips LIVE → CLOSE_ONLY
            # after the first BUY fill, and the canary's own exit (and any
            # orphan re-import exits) happen in CLOSE_ONLY mode. v45 v1
            # gate on `== LIVE` skipped patient flow on every canary exit.
            if (
                mode in (BotMode.LIVE, BotMode.CLOSE_ONLY)
                and self._patient_mode_getter()
            ):
                patient_filled = await self._try_patient_sell(
                    position_id=position_id,
                    entry_intent_id=entry_intent_id,
                    strategy=strategy,
                    market_id=market_id,
                    token_id=token_id,
                    shares=shares,
                    price_hint=price_hint,
                    end_date_iso=end_date_iso,
                )
                if patient_filled:
                    return
        except Exception:
            logger.exception(
                "live_orders: patient-SELL helper raised for position %s "
                "— falling through to legacy FAK", position_id,
            )
            self._stats["patient_sell_errors"] = (
                self._stats.get("patient_sell_errors", 0) + 1
            )

        # Include position_id even when entry_intent_id is set, so
        # imported positions (which reuse `imported:{token_id}` across
        # re-imports of the same token) don't collide on the SELL
        # `:exit` row's UNIQUE client_order_id constraint. Each
        # position gets its own exit suffix.
        client_order_id = (
            f"poly-v3-{entry_intent_id}:pos{position_id}:exit"
            if entry_intent_id
            else f"poly-v3-pos:{position_id}:exit"
        )
        try:
            sign_result = await self._live_client.sign_only(
                token_id=token_id,
                price=price_hint,
                size=shares,
                side="SELL",
            )
        except Exception:
            logger.exception(
                "live_orders: SELL sign failed for position %s (mode=%s)",
                position_id, mode.value,
            )
            self._stats["live_order_errors"] += 1
            # 2026-05-07 PHASE 12 — sign-fail restate. Phase 7 only
            # ran restate_close_failed on submit-fail (escalation
            # exhausted). When sign FAILS (e.g., V2 minimum size,
            # malformed signed_order_json), close_position has
            # already recorded fictional realized_pnl from the limit
            # price. Roll it back so dashboards show truth.
            try:
                await self._positions.restate_close_failed(
                    position_id=position_id,
                )
            except Exception:
                logger.exception(
                    "live_orders: SELL sign-fail restate_close_failed "
                    "raised for position %s", position_id,
                )
            return

        try:
            await self._live_repo.insert(
                LiveOrderRow(
                    intent_id=f"{entry_intent_id}:exit" if entry_intent_id else f"pos:{position_id}:exit",
                    strategy=strategy,
                    market_id=market_id,
                    token_id=token_id,
                    side="SELL",
                    limit_price=price_hint,
                    size_usd=price_hint * shares,
                    shares=shares,
                    # FAK (Fill-And-Kill): if a buyer is at our limit
                    # the SELL fills immediately; otherwise it's
                    # cancelled instead of resting on the orderbook.
                    # Switched from GTC after observing the first
                    # successful EXIT_TP_ABS rest unfilled (operator
                    # asked for immediate-or-cancel SELL semantics so
                    # the on-chain position closes the same instant
                    # the bot's exit decision fires, even at the cost
                    # of missing some fills when no buyer is at the
                    # limit).
                    order_type="FAK",
                    mode=mode.value,
                    client_order_id=client_order_id,
                    signed_order_json=sign_result.signed_order_json,
                    signed_at=now_ts,
                    status="signed",
                )
            )
        except Exception:
            logger.exception(
                "live_orders: SELL persist failed for position %s", position_id
            )
            self._stats["live_order_errors"] += 1
            return
        self._stats["live_orders_signed"] += 1

        # 2026-05-05: CLOSE_ONLY mode submits real SELLs alongside LIVE.
        # LIVE_DRY signs only and stops here.
        if mode not in (BotMode.LIVE, BotMode.CLOSE_ONLY):
            return

        # 2026-05-06 PHASE 5 PART A — min-hold gate.
        # ProfitTaker can fire EXIT_SL_ABS within ~1s of a BUY fill,
        # before Polygon has mined the BUY transaction. Polymarket's
        # exchange contract then refuses the SELL with HTTP 400
        # "balance: 0". Pre-empt the race by waiting until at least
        # SELL_LIVE_MIN_HOLD_S have elapsed since the BUY's
        # submitted_at. This is a one-shot wait per SELL — if the
        # gap is already past, no delay.
        if mode in (BotMode.LIVE, BotMode.CLOSE_ONLY) and entry_intent_id:
            buy_coid_for_settle = f"poly-v3-{entry_intent_id}"
            buy_row_for_settle = await self._live_repo.fetch_by_client_id(
                buy_coid_for_settle
            )
            if buy_row_for_settle is not None:
                buy_submitted_at = buy_row_for_settle.get("submitted_at")
                if buy_submitted_at:
                    elapsed_s = time.time() - float(buy_submitted_at)
                    needed = self._min_hold_s - elapsed_s
                    if needed > 0:
                        logger.info(
                            "live_orders: SELL min-hold wait %.2fs for "
                            "position %s (BUY filled %.2fs ago)",
                            needed, position_id, elapsed_s,
                        )
                        self._stats["sell_min_hold_waits"] = (
                            self._stats.get("sell_min_hold_waits", 0) + 1
                        )
                        await asyncio.sleep(needed)

        # 2026-05-09 PHASE 32 P2 — on-chain inventory pre-check.
        # Even with the 30s min-hold wait above, Polymarket V2's
        # settlement window has been observed >85s (v54 pos 22499).
        # Read the chain balance directly; if it's still 0 (or below
        # 99% of expected), defer the SELL — the next ProfitTaker tick
        # will re-enter this path and try again. Saves one guaranteed
        # `balance: 0` POST and the audit-row noise it produces.
        if mode in (BotMode.LIVE, BotMode.CLOSE_ONLY):
            ok, reason = await _onchain_inventory_ok(
                check=self._onchain_inventory_check,
                token_id=token_id,
                expected_shares=float(shares),
            )
            if not ok:
                logger.info(
                    "live_orders: deferring SELL for position %s — "
                    "on-chain inventory pre-check failed (%s)",
                    position_id, reason,
                )
                self._stats["sell_deferred_no_onchain_inventory"] = (
                    self._stats.get(
                        "sell_deferred_no_onchain_inventory", 0
                    ) + 1
                )
                return

        # SELL escalation loop. With FAK the order either matches the
        # book immediately or gets killed by Polymarket with "no
        # orders found to match". When killed, we undercut the price
        # by SELL_ESCALATION_UNDERCUT_PCT (default 5%), wait
        # SELL_ESCALATION_WAIT_S, sign+post a fresh attempt with a
        # new `:exit:retry{N}` client_order_id, and try again — up
        # to SELL_ESCALATION_MAX_ATTEMPTS or until price would drop
        # below SELL_ESCALATION_MIN_PRICE.
        #
        # 2026-05-06 PHASE 5 PART B — chain-settle balance retry.
        # If POST fails with "balance: 0" / "balance is not enough"
        # (indicating the BUY hasn't yet settled on Polygon), retry
        # at the SAME price with exponential backoff
        # (SELL_BALANCE_RETRY_BASE_DELAY_S × 2^attempt). Bounded
        # by SELL_BALANCE_RETRY_MAX_ATTEMPTS. Distinct from
        # FAK no-match (which lowers price) — balance:0 is a timing
        # issue, not a price issue.
        current_price = price_hint
        current_client_order_id = client_order_id

        # 2026-05-08 PHASE 19 — fresh best_bid re-quote.
        # Mirror of the BUY path's best_ask re-quote (~line 460).
        # tick_poller's price_hint is up to 5s stale; on thin books
        # the bid level moves in <2s. Re-query best_bid AT submit
        # time and cross by SELL_REQUOTE_BUFFER_PCT under it. Bound
        # the slip below price_hint by SELL_MAX_SLIP_PCT so a
        # collapsed book doesn't trigger a fire-sale, and clamp at
        # the entry-relative floor (Phase 15) so we never SELL
        # below 10% of entry.
        if mode is BotMode.LIVE and self._live_client is not None:
            try:
                fresh_bid = await asyncio.to_thread(
                    self._live_client.get_best_bid, token_id
                )
            except Exception:
                fresh_bid = None
            if fresh_bid is not None and fresh_bid > 0:
                slip_floor = float(price_hint) * (
                    1.0 - self.SELL_MAX_SLIP_PCT
                )
                entry_floor = max(
                    self.SELL_ESCALATION_MIN_PRICE,
                    entry_price * self.SELL_ESCALATION_MIN_PRICE_RATIO,
                )
                target = float(fresh_bid) * (
                    1.0 - self.SELL_REQUOTE_BUFFER_PCT
                )
                # Final price is bounded:
                #   upper bound: original price_hint (don't waste profit)
                #   lower bound: max(entry_floor, slip_floor)
                effective_floor = max(entry_floor, slip_floor)
                requoted = max(effective_floor, min(target, float(price_hint)))
                if abs(requoted - float(price_hint)) > 1e-9:
                    logger.info(
                        "live_orders: SELL re-quote for position %s — "
                        "price_hint=%.4f best_bid=%.4f → effective=%.4f "
                        "(entry=%.4f, entry_floor=%.4f, slip_floor=%.4f) "
                        "[Phase 19]",
                        position_id, float(price_hint), float(fresh_bid),
                        requoted, entry_price, entry_floor, slip_floor,
                    )
                    self._stats["sell_requoted_phase19"] = (
                        self._stats.get("sell_requoted_phase19", 0) + 1
                    )
                current_price = requoted

        no_match_fragment = "no orders found to match"
        balance_zero_fragments = (
            "balance: 0",
            "balance is not enough",
        )
        attempt = 0
        balance_retry_attempt = 0
        while True:
            try:
                submit_result = await self._live_client.sign_and_submit(
                    token_id=token_id,
                    price=current_price,
                    size=shares,
                    side="SELL",
                    order_type="FAK",
                )
            except Exception as exc:
                err_lower = str(exc).lower()
                is_no_match = no_match_fragment in err_lower
                # 2026-05-07 PHASE 18 — disambiguate "matching in
                # flight" from "balance: 0". Both share the same
                # parent error class; only the message body
                # distinguishes them. See class docstring above.
                is_matching_in_flight = (
                    self.SELL_MATCHING_IN_FLIGHT_FRAGMENT in err_lower
                )
                is_balance_zero = (
                    any(frag in err_lower for frag in balance_zero_fragments)
                    and not is_matching_in_flight
                )
                logger.warning(
                    "live_orders: SELL POST failed (attempt=%d, "
                    "price=%.4f, no_match=%s, balance_zero=%s, "
                    "matching_in_flight=%s): %s",
                    attempt, current_price, is_no_match,
                    is_balance_zero, is_matching_in_flight, exc,
                )
                try:
                    await self._live_repo.mark_rejected(
                        client_order_id=current_client_order_id,
                        response_json=json.dumps({"error": str(exc)}),
                        ts=int(time.time()),
                    )
                except Exception:
                    logger.exception(
                        "live_orders: mark_rejected failed for %s",
                        current_client_order_id,
                    )

                # 2026-05-07 PHASE 18 — matching-in-flight reconcile.
                # Polymarket's engine matched a previous SELL we
                # submitted; the response body says so via
                # "sum of matched orders". Don't retry (would just
                # collide with the inflight match). Poll the trades
                # API for our fill and restate the close_price to
                # the actual on-chain price.
                #
                # When trades_fetcher is None (Phase 18 disabled at
                # construction time), preserve pre-Phase-18 behavior
                # by treating the error as a balance-zero retry.
                if is_matching_in_flight and self._trades_fetcher is None:
                    is_balance_zero = True  # fall through to legacy retry
                if is_matching_in_flight and self._trades_fetcher is not None:
                    self._stats["sell_matching_in_flight"] = (
                        self._stats.get("sell_matching_in_flight", 0) + 1
                    )
                    logger.info(
                        "live_orders: SELL matching-in-flight detected "
                        "for position %s — polling trades API for "
                        "actual fill (Phase 18 reconcile)",
                        position_id,
                    )
                    fill = await self._reconcile_sell_via_trades(
                        token_id=token_id,
                        target_size=shares,
                        since_ts=now_ts - 60,
                    )
                    if fill is not None:
                        actual_price = float(fill["price"])
                        actual_size = float(fill["size"])
                        tx_hash = str(fill.get("transactionHash", ""))
                        try:
                            await self._positions.restate_close_price(
                                position_id=position_id,
                                actual_exit_price=actual_price,
                            )
                        except Exception:
                            logger.exception(
                                "live_orders: Phase 18 restate failed "
                                "for position %s", position_id,
                            )
                        self._stats["sell_reconciled_via_trades"] = (
                            self._stats.get(
                                "sell_reconciled_via_trades", 0
                            ) + 1
                        )
                        logger.info(
                            "live_orders: SELL reconciled via trades "
                            "for position %s — actual price=$%.4f "
                            "size=%.4f tx=%s (intent was %.4f sh × "
                            "$%.4f)",
                            position_id, actual_price, actual_size,
                            tx_hash[:12], shares, current_price,
                        )
                        return
                    # /trades didn't surface our fill within timeout —
                    # fall through to balance-retry. Could be a real
                    # chain-settle delay where the matching engine's
                    # response was a false positive on "in flight".
                    logger.warning(
                        "live_orders: Phase 18 reconcile found no "
                        "matching trade for position %s within %.1fs "
                        "— falling back to balance-zero retry",
                        position_id, self.SELL_RECONCILE_TIMEOUT_S,
                    )
                    is_balance_zero = True  # fall through

                # 2026-05-06 PHASE 5 PART B — balance-zero retry.
                # Chain settle race: BUY hasn't yet been mined into
                # Polygon, so the exchange contract's balanceOf check
                # refuses the SELL. Retry at SAME price with
                # exponential backoff. Distinct from FAK no-match.
                if is_balance_zero:
                    balance_retry_attempt += 1
                    if balance_retry_attempt >= self.SELL_BALANCE_RETRY_MAX_ATTEMPTS:
                        self._stats["sell_balance_zero_exhausted"] = (
                            self._stats.get("sell_balance_zero_exhausted", 0) + 1
                        )
                        logger.warning(
                            "live_orders: SELL balance-zero retry "
                            "exhausted for position %s after %d "
                            "attempts (price=%.4f) — chain may not "
                            "settle in time; importer will pick up "
                            "leftover shares",
                            position_id,
                            balance_retry_attempt,
                            current_price,
                        )
                        # Phase 7 — undo phantom realized_pnl set by
                        # close_position. The SELL never filled, so
                        # the position should NOT show a realized
                        # gain/loss yet. Importer will reconstruct.
                        try:
                            await self._positions.restate_close_failed(
                                position_id=position_id,
                            )
                        except Exception:
                            logger.exception(
                                "live_orders: restate_close_failed "
                                "failed for position %s (balance "
                                "exhausted path)", position_id,
                            )
                        return
                    backoff = (
                        self.SELL_BALANCE_RETRY_BASE_DELAY_S
                        * (2 ** (balance_retry_attempt - 1))
                    )
                    logger.info(
                        "live_orders: SELL balance-zero retry %d/%d "
                        "for position %s in %.1fs (same price=%.4f)",
                        balance_retry_attempt,
                        self.SELL_BALANCE_RETRY_MAX_ATTEMPTS,
                        position_id, backoff, current_price,
                    )
                    self._stats["sell_balance_zero_retries"] = (
                        self._stats.get("sell_balance_zero_retries", 0) + 1
                    )
                    await asyncio.sleep(backoff)
                    # Sign + audit-insert a fresh row for the retry —
                    # same price, but a unique client_order_id so the
                    # audit table can disambiguate retries.
                    try:
                        retry_sign = await self._live_client.sign_only(
                            token_id=token_id,
                            price=current_price,
                            size=shares,
                            side="SELL",
                        )
                    except Exception:
                        logger.exception(
                            "live_orders: SELL balance-retry sign "
                            "failed for position %s", position_id,
                        )
                        self._stats["live_order_errors"] += 1
                        return
                    retry_intent_id = (
                        f"{entry_intent_id}:exit:bal_retry{balance_retry_attempt}"
                        if entry_intent_id
                        else f"pos:{position_id}:exit:bal_retry{balance_retry_attempt}"
                    )
                    current_client_order_id = (
                        f"{client_order_id}:bal_retry{balance_retry_attempt}"
                    )
                    try:
                        await self._live_repo.insert(
                            LiveOrderRow(
                                intent_id=retry_intent_id,
                                strategy=strategy,
                                market_id=market_id,
                                token_id=token_id,
                                side="SELL",
                                limit_price=current_price,
                                size_usd=current_price * shares,
                                shares=shares,
                                order_type="FAK",
                                mode=mode.value,
                                client_order_id=current_client_order_id,
                                signed_order_json=retry_sign.signed_order_json,
                                signed_at=int(time.time()),
                                status="signed",
                            )
                        )
                    except Exception:
                        logger.exception(
                            "live_orders: SELL balance-retry persist "
                            "failed for position %s", position_id,
                        )
                        return
                    continue  # retry at same price

                if not is_no_match:
                    # Real submission error — don't escalate.
                    self._stats["live_order_errors"] += 1
                    return
                # Escalate: try a lower price.
                attempt += 1
                if attempt >= self.SELL_ESCALATION_MAX_ATTEMPTS:
                    self._stats["sell_escalation_exhausted"] += 1
                    logger.warning(
                        "live_orders: SELL escalation exhausted for "
                        "position %s after %d attempts (last_price=%.4f)",
                        position_id, attempt, current_price,
                    )
                    # Phase 7 — undo phantom realized_pnl
                    try:
                        await self._positions.restate_close_failed(
                            position_id=position_id,
                        )
                    except Exception:
                        logger.exception(
                            "live_orders: restate_close_failed failed "
                            "for position %s (escalation exhausted "
                            "path)", position_id,
                        )
                    # 2026-05-09 PHASE 31 — set reconciliation lock so
                    # the position_importer skips the leftover on-chain
                    # shares for this token. Without this, v50r2/v51/v52
                    # all saw the importer create phantom-double rows
                    # (22493/22495/22497) for the SAME shares, double-
                    # counting cost basis until the truth-up corrected.
                    if self._reconciliation_lock_repo is not None:
                        try:
                            await self._reconciliation_lock_repo.upsert(
                                token_id=token_id,
                                position_id=position_id,
                                reason="SELL_ESCALATION_EXHAUSTED",
                                created_at=int(time.time()),
                            )
                            self._stats["reconciliation_locks_set"] = (
                                self._stats.get(
                                    "reconciliation_locks_set", 0
                                ) + 1
                            )
                        except Exception:
                            logger.exception(
                                "live_orders: reconciliation_lock_repo "
                                "upsert failed for position %s "
                                "(token=%s, non-fatal)",
                                position_id, token_id,
                            )
                    return
                # 2026-05-08 PHASE 20 — adaptive escalation step.
                # Phase 19 only re-quotes the FIRST submission; legacy
                # subsequent steps undercut prev × 2%. v34/v37 showed
                # that on thin penny binaries the bid can collapse
                # 50-78% in seconds — a 2% step never catches contra
                # and the position strands (SELL_FAILED). Phase 20
                # re-queries best_bid on EACH retry and takes the
                # MORE aggressive of (legacy 2%-step) vs (fresh_bid -
                # 0.5% buffer). On a stable bid the 2%-step still
                # wins (no overshoot down). On a fast collapse the
                # fresh re-quote drops next_price to track the bid.
                legacy_step = current_price * (
                    1 - self.SELL_ESCALATION_UNDERCUT_PCT
                )
                fresh_bid_retry: float | None = None
                try:
                    fresh_bid_retry = await asyncio.to_thread(
                        self._live_client.get_best_bid, token_id
                    )
                except Exception:
                    fresh_bid_retry = None
                if fresh_bid_retry is not None and fresh_bid_retry > 0:
                    requote_target = float(fresh_bid_retry) * (
                        1.0 - self.SELL_REQUOTE_BUFFER_PCT
                    )
                    next_price = min(legacy_step, requote_target)
                    if next_price < legacy_step:
                        self._stats["sell_requoted_phase20"] = (
                            self._stats.get("sell_requoted_phase20", 0)
                            + 1
                        )
                        logger.info(
                            "live_orders: SELL Phase 20 retry "
                            "re-quote pos=%s attempt=%d "
                            "legacy_step=%.4f fresh_bid=%.4f "
                            "buffered_target=%.4f → next=%.4f",
                            position_id, attempt, legacy_step,
                            float(fresh_bid_retry), requote_target,
                            next_price,
                        )
                else:
                    next_price = legacy_step
                # 2026-05-07 PHASE 15 — entry-relative floor.
                # effective_floor = max(absolute tick floor,
                #                       entry_price × ratio).
                # Legacy hardcoded $0.05 stranded penny-entry
                # positions (v27 / pos 22445) because the very first
                # retry's projected price was below $0.05. Scaling
                # with entry_price keeps the safety semantics for
                # normal markets (entry 0.50 → floor 0.05) while
                # allowing penny markets to escalate down toward
                # the Polymarket tick minimum (entry 0.01 →
                # floor 0.001). When entry_price is unknown (legacy
                # call sites that don't pass it), the absolute floor
                # alone applies — preserves prior behavior.
                effective_floor = max(
                    self.SELL_ESCALATION_MIN_PRICE,
                    entry_price * self.SELL_ESCALATION_MIN_PRICE_RATIO,
                )
                if next_price < effective_floor:
                    self._stats["sell_escalation_exhausted"] += 1
                    logger.warning(
                        "live_orders: SELL escalation hit min-price "
                        "floor %.4f for position %s (entry=%.4f, "
                        "abs=%.4f, ratio=%.2f)",
                        effective_floor, position_id, entry_price,
                        self.SELL_ESCALATION_MIN_PRICE,
                        self.SELL_ESCALATION_MIN_PRICE_RATIO,
                    )
                    # Phase 7 — also restate on min-price floor exit
                    try:
                        await self._positions.restate_close_failed(
                            position_id=position_id,
                        )
                    except Exception:
                        logger.exception(
                            "live_orders: restate_close_failed failed "
                            "for position %s (min-price floor path)",
                            position_id,
                        )
                    return
                await asyncio.sleep(self.SELL_ESCALATION_WAIT_S)
                # Sign + audit-insert a NEW row for this retry. New
                # client_order_id so the inventory gate / reconciler
                # can disambiguate retry attempts in the audit table.
                try:
                    retry_sign = await self._live_client.sign_only(
                        token_id=token_id,
                        price=next_price,
                        size=shares,
                        side="SELL",
                    )
                except Exception:
                    logger.exception(
                        "live_orders: SELL retry sign failed for "
                        "position %s", position_id,
                    )
                    self._stats["live_order_errors"] += 1
                    return
                retry_intent_id = (
                    f"{entry_intent_id}:exit:retry{attempt}"
                    if entry_intent_id
                    else f"pos:{position_id}:exit:retry{attempt}"
                )
                current_client_order_id = (
                    f"{client_order_id}:retry{attempt}"
                )
                try:
                    await self._live_repo.insert(
                        LiveOrderRow(
                            intent_id=retry_intent_id,
                            strategy=strategy,
                            market_id=market_id,
                            token_id=token_id,
                            side="SELL",
                            limit_price=next_price,
                            size_usd=next_price * shares,
                            shares=shares,
                            order_type="FAK",
                            mode=mode.value,
                            client_order_id=current_client_order_id,
                            signed_order_json=retry_sign.signed_order_json,
                            signed_at=int(time.time()),
                            status="signed",
                        )
                    )
                    self._stats["sell_escalation_retries"] += 1
                except Exception:
                    logger.exception(
                        "live_orders: SELL retry persist failed for "
                        "position %s", position_id,
                    )
                    return
                current_price = next_price
                continue  # re-enter loop with the lower price

            # Submission succeeded — mark and exit loop.
            try:
                await self._live_repo.mark_submitted(
                    client_order_id=current_client_order_id,
                    response_json=json.dumps(submit_result.response or {}),
                    submitted_at=int(time.time()),
                )
            except Exception:
                logger.exception(
                    "live_orders: SELL mark_submitted failed for %s",
                    current_client_order_id,
                )
                self._stats["live_order_errors"] += 1
                return
            polymarket_order_id = _extract_polymarket_order_id(
                submit_result.response
            )
            if polymarket_order_id:
                try:
                    await self._live_repo.set_polymarket_order_id(
                        client_order_id=current_client_order_id,
                        polymarket_order_id=polymarket_order_id,
                        ts=int(time.time()),
                    )
                except Exception:
                    logger.exception(
                        "live_orders: SELL set_polymarket_order_id "
                        "failed for %s", current_client_order_id,
                    )

            # 2026-05-06 PHASE 4 — eager SELL record_fill + restate.
            # Polymarket POST /order returns synchronously-matched
            # SELLs with takingAmount/makingAmount. The bot used to
            # walk away assuming User-WS would fire EVT_ORDER_FILLED
            # later — but for synchronous matches that event often
            # doesn't arrive, leaving live_orders.filled_qty=0 and
            # the position's realized_pnl stuck at the limit-price
            # value. Mirror the BUY-side eager record_fill, AND
            # restate the position's exit_price so realized_pnl
            # reflects the actual on-chain fill.
            sell_fill = _extract_sell_fill(
                submit_result.response, current_price
            )
            if sell_fill is not None and polymarket_order_id:
                filled_shares, avg_fill_price = sell_fill
                try:
                    await self._live_repo.record_fill(
                        polymarket_order_id=polymarket_order_id,
                        fill_qty=filled_shares,
                        fill_price=avg_fill_price,
                        ts=int(time.time()),
                        terminal=True,
                    )
                except Exception:
                    logger.exception(
                        "live_orders: SELL eager record_fill failed "
                        "for %s — LiveFillReconciler will retry "
                        "via UserWS",
                        polymarket_order_id,
                    )
                    self._stats["live_order_errors"] += 1
                # Restate the position's exit_price/realized_pnl with
                # the actual fill so dashboards + bilan are correct.
                try:
                    restate = await self._positions.restate_close_price(
                        position_id=position_id,
                        actual_exit_price=avg_fill_price,
                    )
                    if restate is not None and (
                        restate.get("prior_exit_price")
                        != avg_fill_price
                    ):
                        logger.info(
                            "live_orders: SELL restated position %s "
                            "exit_price %.4f → %.4f, realized_pnl "
                            "%.4f → %.4f (actual on-chain fill)",
                            position_id,
                            float(restate.get("prior_exit_price") or 0),
                            avg_fill_price,
                            float(restate.get("prior_realized_pnl") or 0),
                            float(restate.get("realized_pnl") or 0),
                        )
                except Exception:
                    logger.exception(
                        "live_orders: SELL restate_close_price failed "
                        "for position %s", position_id,
                    )

            self._stats["live_orders_submitted"] += 1
            if attempt > 0:
                self._stats["sell_escalation_filled"] += 1
                logger.info(
                    "live_orders: SELL escalation FILLED for position "
                    "%s after %d retries (final_price=%.4f, "
                    "original=%.4f)",
                    position_id, attempt, current_price, price_hint,
                )
            return
