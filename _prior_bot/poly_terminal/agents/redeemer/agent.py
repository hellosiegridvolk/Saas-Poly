"""RedeemerAgent — classify closed-but-unredeemed positions.

Polymarket V2 binary outcomes settle on-chain via
`ConditionalTokens.redeemPositions(...)`. We can't yet automate
the actual redemption call because:

  - The funder is a Magic-Link proxy (signature_type=1), so direct
    web3 calls from the EOA target the wrong holder.
  - Polymarket's relayer (`py-builder-relayer-client`) is the
    correct path but requires separately-issued BUILDER_API_KEY
    creds we don't have, and the package still references V1
    (USDC.e) addresses as of writing.

Until either path lands, this agent:

  1. Polls `positions WHERE closed_ts IS NOT NULL AND redeemed_ts
     IS NULL` every `interval_s` seconds.
  2. For each unique market_id, queries Gamma
     `?condition_ids={cid}&closed=true` to find the resolution.
  3. Classifies each position:
       - PENDING   → market not closed yet; skip until next sweep.
       - WORTHLESS → we held the losing outcome; auto-mark
         `redeemed_ts=now, redeem_tx_hash='WORTHLESS_NO_TX'` so the
         queue clears without touching chain.
       - REDEEMABLE → we held the winning outcome; track the $
         total in `stats` so the operator (or `/status` endpoint)
         knows there's money to claim. NO state mutation here —
         the row stays unredeemed until the manual redemption is
         logged by `scripts/redeem_status.py --confirm-redeemed`.
  4. Logs a WARN every `nudge_interval_s` if the REDEEMABLE total
     exceeds `nudge_threshold_usd`.

Forward-compat: when the relayer/web3 redemption flow lands, swap
the REDEEMABLE branch's "log + accumulate" for "build calldata,
submit, capture tx hash, mark_redeemed". The classification logic
stays identical.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)


def _coerce_list(value: Any) -> list[str]:
    """Gamma serializes list-typed market fields as JSON-encoded
    strings (e.g., `'["108…", "112…"]'` for `clobTokenIds`). Pass
    those through json.loads; pass real lists through unchanged.
    Returns a plain list of str — empty on any parse error.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return []
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    return []


# Sentinel hashes for positions cleared without an on-chain tx.
# Intentionally non-hex so any downstream check that tries to look up
# the tx on-chain refuses.
WORTHLESS_NO_TX = "WORTHLESS_NO_TX"  # we held losing side at resolution
PAPER_NO_TX = "PAPER_NO_TX"          # PAPER position; no on-chain inventory
# 2026-05-11 PHASE 36 — relayer failed for a high-payout position so
# many times we gave up. The position is "redeemed" from the agent's
# point of view (no more sweeps will pick it up), but on-chain shares
# remain. Operator can clear this sentinel to retry, OR claim via the
# Polymarket UI directly.
RELAYER_BLOCKED = "RELAYER_BLOCKED"


class _PositionsRepo(Protocol):
    async def fetch_closed_unredeemed(self) -> list[dict[str, object]]: ...
    async def mark_redeemed(
        self, position_id: int, redeemed_ts: int, redeem_tx_hash: str
    ) -> bool: ...


class _LiveOrdersRepo(Protocol):
    async def fetch_by_client_id(
        self, client_order_id: str
    ) -> dict[str, object] | None: ...


class _MarketResolver(Protocol):
    async def fetch_resolution(
        self, condition_id: str
    ) -> dict[str, Any] | None:
        """Return the Gamma `/markets` row for `condition_id` if the
        market is resolved, else None."""
        ...


@dataclass(frozen=True)
class RedeemerConfig:
    # 60s gives sub-minute resolution detection without hammering
    # Gamma — markets resolve once, not continuously, so 30s buys
    # very little over 60s while doubling the request load.
    interval_s: float = 60.0
    nudge_interval_s: float = 1800.0   # 30 min between WARN nudges
    nudge_threshold_usd: float = 5.0   # nudge if redeemable > $5
    max_concurrent_lookups: int = 4    # Gamma /markets is ~30 req/s
    # 2026-05-03 P2 #4 fix (deep-research-report 14/15/16/17 §redeemer-backoff):
    # quarantine condition IDs that fail resolver lookup repeatedly so
    # we don't burn Gamma quota on neg-risk / sub-cent / unsupported
    # markets that just won't resolve. After this many consecutive
    # failures, the resolver call is skipped (with a periodic WARN
    # log every quarantine_warn_every sweeps so it stays visible).
    quarantine_after_failures: int = 5
    quarantine_warn_every: int = 60   # ~1 hour between WARN reminders
    # 2026-05-07 PHASE 16 — auto-redeem.
    # When True AND a `relayer_redeemer` is wired, the REDEEMABLE
    # branch builds + submits the on-chain redemption via the
    # Polymarket relayer (CTF for standard markets, NegRiskAdapter
    # for neg-risk). Default False — explicit opt-in protects against
    # accidental live submission while the integration is fresh.
    # The redeemer's own `dry_run` flag controls whether actual
    # transactions land or only calldata is logged; both gates must
    # agree (auto_enabled AND not dry_run) before real funds move.
    auto_enabled: bool = False
    # When auto_enabled but submission raises, requeue the position
    # for the next sweep instead of leaving it as REDEEMABLE in stats
    # forever. Bounded retries are handled by the per-condition-id
    # quarantine counter that's already used for resolver failures.
    auto_max_retries_per_position: int = 3
    # 2026-05-09 PHASE 32 — WORTHLESS_NO_TX retry-cap fallback.
    # When True AND a position has exhausted `auto_max_retries_per_position`
    # AND its estimated payout is below `worthless_no_tx_payout_ceiling_usd`,
    # the agent stops counting it as REDEEMABLE and force-marks
    # `redeem_tx_hash=WORTHLESS_NO_TX`. This unblocks the Phase 30
    # truth-up so the cost-basis loss is recorded instead of leaving
    # the row stuck REDEEMABLE indefinitely (v54 pos 22499 motivation).
    #
    # Positions with payout >= the ceiling are NEVER auto-cleared —
    # they wait for manual operator intervention to avoid forfeiting
    # real value. Default OFF so the flag is opt-in.
    worthless_no_tx_after_cap: bool = False
    worthless_no_tx_payout_ceiling_usd: float = 1.00
    # 2026-05-10 PHASE 33 — PAPER truth-up after market resolution.
    # When True, PAPER positions (no on-chain inventory) are still
    # consulted against Gamma for resolution. If the held side
    # resolved against us, realized_pnl is truth-up'd to −cost_basis;
    # if it won, realized_pnl is truth-up'd to (shares × $1 − cost).
    # Default OFF preserves legacy short-circuit behavior; PAPER soaks
    # should flip this on so accounting matches the LIVE-truth model.
    # Motivator: 14.7h soak (2026-05-09) had pos 22650 + 22736 close
    # via TIME at break-even price → realized=$0, hiding $6.74 of
    # real-economic loss when the bars resolved against the held side.
    paper_truth_up_enabled: bool = False
    # 2026-05-11 PHASE 36 — block-after-retry-cap fallback. When True
    # (default), positions that exhaust `auto_max_retries_per_position`
    # AND aren't escalated to WORTHLESS_NO_TX get marked with the
    # `RELAYER_BLOCKED` sentinel. This removes them from the sweep
    # working set so subsequent sweeps + bot restarts don't re-fire the
    # same retry-failure errors (4 positions × 3 retries = 12 errors
    # per restart observed on 2026-05-11 before this fix).
    #
    # The on-chain shares remain claimable — operator can clear the
    # sentinel via `UPDATE positions SET redeem_tx_hash=NULL,
    # redeemed_ts=NULL WHERE position_id=...` to retry, or claim via
    # the Polymarket UI directly. realized_pnl is NOT touched.
    block_after_retry_cap: bool = True


@dataclass
class RedeemerStats:
    sweeps: int = 0
    pending: int = 0           # current PENDING count (last sweep)
    worthless_marked: int = 0  # cumulative WORTHLESS auto-marks
    paper_skipped: int = 0     # cumulative PAPER auto-marks (no on-chain)
    redeemable_count: int = 0  # current REDEEMABLE count (last sweep)
    redeemable_usd: float = 0  # current REDEEMABLE total $ (last sweep)
    last_sweep_ts: int = 0
    last_nudge_ts: int = 0
    errors: int = 0
    quarantined_condition_ids: int = 0  # current quarantine size
    # 2026-05-07 PHASE 16 — auto-redeem instrumentation
    auto_redeemed: int = 0           # cumulative successful submissions
    auto_redeem_errors: int = 0      # cumulative failed submissions
    auto_redeemed_usd: float = 0.0   # cumulative $ value redeemed
    # 2026-05-09 PHASE 32 — relayer give-up escalations (low-payout
    # positions auto-marked WORTHLESS_NO_TX after retry cap exhaustion).
    relayer_giveup_marks_worthless: int = 0
    # 2026-05-11 PHASE 36 — high-payout positions blocked after retry
    # cap (marked RELAYER_BLOCKED to exit sweep loop without claiming
    # worthless). On-chain shares remain available for manual recovery.
    relayer_blocked: int = 0


class RedeemerAgent:
    """Periodic redemption-status sweeper. See module docstring."""

    def __init__(
        self,
        positions_repo: _PositionsRepo,
        market_resolver: _MarketResolver,
        cfg: RedeemerConfig | None = None,
        live_orders_repo: _LiveOrdersRepo | None = None,
        relayer_redeemer: Any | None = None,
        # 2026-05-09 PHASE 31 — clear reconciliation locks after a
        # successful redeem (worthless or REDEEMABLE on-chain).
        # The lock was set by ExecutionAgent on SELL_FAILED to keep
        # the importer from re-importing leftover on-chain shares;
        # once the redeem resolves the underlying position the lock
        # must be released so future imports of NEW shares on the
        # same token aren't blocked.
        reconciliation_lock_repo: Any | None = None,
    ) -> None:
        self._positions = positions_repo
        self._resolver = market_resolver
        self._live_orders = live_orders_repo
        self._reconciliation_lock_repo = reconciliation_lock_repo
        self._cfg = cfg or RedeemerConfig()
        self._stats = RedeemerStats()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._sem: asyncio.Semaphore | None = None
        # 2026-05-03 P2 #4: track per-condition-id failure count so we
        # can quarantine markets the resolver can't handle (neg-risk,
        # sub-cent ticks, and similar edge cases that previously
        # cycled forever — see deep-research-report 14/15/16/17).
        self._fail_counts: dict[str, int] = {}
        # 2026-05-07 PHASE 16: optional auto-redeem submitter. When
        # None, the REDEEMABLE branch falls back to the legacy "log +
        # accumulate" behavior so operators continue to redeem
        # manually via Polymarket's UI. When wired AND
        # `cfg.auto_enabled` is True, the agent submits the redemption
        # on-chain (or as DRY_RUN_REDEEM:<cid> when the redeemer's
        # own dry_run=True). Per-position retry counter prevents a
        # flaky relayer from looping forever; on retry exhaustion the
        # position falls back to "leave as REDEEMABLE" for manual.
        self._relayer = relayer_redeemer
        self._auto_retry_counts: dict[int, int] = {}
        # 2026-05-09 PHASE 31 P1a — retry-cap warning de-dupe.
        # Pre-fix: every sweep (~62s) re-discovered the same stuck
        # REDEEMABLE position, hit the retry cap, and re-warned.
        # v54 saw 150+ identical warnings in 57min (~2.8 lines/min).
        # Now we warn ONCE per (position_id, process lifetime); the
        # set is cleared on successful redeem or process restart.
        self._warned_retry_cap: set[int] = set()

    @property
    def stats(self) -> RedeemerStats:
        return self._stats

    async def _mark_redeemed_and_clear_lock(
        self,
        *,
        position_id: int,
        redeemed_ts: int,
        redeem_tx_hash: str,
        payout_usd: float | None = None,
    ) -> bool:
        """Phase 31 — atomic: mark redeemed AND clear any reconciliation
        lock for this position. Single helper so every mark_redeemed
        call site benefits.

        `payout_usd` is forwarded so PositionsRepo.mark_redeemed can
        truth-up realized_pnl on real-tx-zero-payout redeems
        (Phase 31 P0c — closes the v50r2 pos 22492 silent-loss case).

        Returns the result of the underlying mark_redeemed (False if
        the row was already marked — race-safe).
        """
        ok = await self._positions.mark_redeemed(
            position_id=position_id,
            redeemed_ts=redeemed_ts,
            redeem_tx_hash=redeem_tx_hash,
            payout_usd=payout_usd,
        )
        if ok and self._reconciliation_lock_repo is not None:
            try:
                await self._reconciliation_lock_repo.clear(
                    position_id=position_id,
                    cleared_at=redeemed_ts,
                )
            except Exception:
                logger.exception(
                    "redeemer: reconciliation_lock_repo.clear failed "
                    "for position %s (non-fatal; lock will expire on "
                    "TTL)", position_id,
                )
        return ok

    async def start(self) -> None:
        if self._task is not None:
            return
        self._sem = asyncio.Semaphore(self._cfg.max_concurrent_lookups)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _loop(self) -> None:
        # First sweep runs immediately so /status reflects accurate
        # state on boot. After that, honor the configured interval.
        try:
            await self.run_once()
        except Exception:
            logger.exception("redeemer: initial sweep failed")
            self._stats.errors += 1
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._cfg.interval_s
                )
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                return
            try:
                await self.run_once()
            except Exception:
                logger.exception("redeemer: sweep failed")
                self._stats.errors += 1

    async def run_once(self) -> RedeemerStats:
        """One sweep: fetch unredeemed, gate by on-chain inventory,
        classify the remainder, mark WORTHLESS, accumulate REDEEMABLE
        total, optionally nudge.

        Public so `scripts/redeem_status.py` can drive it manually
        without spinning up the polling loop.
        """
        rows = await self._positions.fetch_closed_unredeemed()
        if not rows:
            self._stats.sweeps += 1
            self._stats.pending = 0
            self._stats.redeemable_count = 0
            self._stats.redeemable_usd = 0.0
            self._stats.last_sweep_ts = int(time.time())
            return self._stats

        # Inventory gate: positions never backed by a successful LIVE
        # BUY (PAPER positions, LIVE BUYs that rejected at submit, or
        # resting LIVE BUYs that never filled) have no on-chain
        # tokens to redeem. Auto-mark them with PAPER_NO_TX so they
        # leave the queue cleanly without falsely showing up as $X
        # of "redeemable" money the operator would chase. Without
        # this gate, every paper run accumulates ghost dollars in
        # the REDEEMABLE total — caught by the first live smoke
        # test against the real DB ($8512 of phantom payout).
        on_chain_rows: list[dict[str, object]] = []
        # 2026-05-10 PHASE 33 — PAPER positions that still need
        # Gamma resolution + truth-up. Non-empty only when
        # `paper_truth_up_enabled=True`. These rows go through the
        # SAME Gamma resolution path as on-chain rows; the marking
        # step uses PAPER_NO_TX (not WORTHLESS_NO_TX) since there's
        # no on-chain inventory.
        paper_truth_up_rows: list[dict[str, object]] = []
        now_ts = int(time.time())
        paper_skipped_this_sweep = 0
        if self._live_orders is not None:
            for r in rows:
                intent_id = str(r["entry_intent_id"] or "")
                if not intent_id:
                    has_inventory = False
                else:
                    buy_row = await self._live_orders.fetch_by_client_id(
                        f"poly-v3-{intent_id}"
                    )
                    has_inventory = (
                        buy_row is not None
                        and float(buy_row.get("filled_qty") or 0) > 0
                    )
                if has_inventory:
                    on_chain_rows.append(r)
                elif self._cfg.paper_truth_up_enabled:
                    # PAPER position + truth-up enabled → defer marking
                    # until Gamma confirms the resolution. The same
                    # resolution loop below now classifies PAPER and
                    # on-chain rows alike.
                    paper_truth_up_rows.append(r)
                else:
                    ok = await self._mark_redeemed_and_clear_lock(
                        position_id=int(r["position_id"]),
                        redeemed_ts=now_ts,
                        redeem_tx_hash=PAPER_NO_TX,
                    )
                    if ok:
                        paper_skipped_this_sweep += 1
        else:
            # No live_orders repo wired (e.g., test or PAPER-only build).
            # Fall through with all rows so the agent stays useful, but
            # the $ totals will be theoretical, not on-chain.
            on_chain_rows = list(rows)

        self._stats.paper_skipped += paper_skipped_this_sweep
        # Phase 33 — paper rows + on-chain rows share the resolution
        # path; only the marking step diverges (PAPER_NO_TX vs real).
        all_resolution_rows = on_chain_rows + paper_truth_up_rows
        paper_position_ids = {
            int(r["position_id"]) for r in paper_truth_up_rows
        }
        if not all_resolution_rows:
            self._stats.sweeps += 1
            self._stats.pending = 0
            self._stats.redeemable_count = 0
            self._stats.redeemable_usd = 0.0
            self._stats.last_sweep_ts = now_ts
            return self._stats

        # Group positions by market_id so we Gamma-fetch each market
        # once even if we hold multiple positions in the same market.
        by_market: dict[str, list[dict[str, object]]] = {}
        for r in all_resolution_rows:
            by_market.setdefault(str(r["market_id"]), []).append(r)

        sem = self._sem or asyncio.Semaphore(self._cfg.max_concurrent_lookups)

        async def _fetch(cid: str) -> tuple[str, dict[str, Any] | None]:
            # 2026-05-03 P2 #4: skip quarantined condition IDs.
            fails = self._fail_counts.get(cid, 0)
            if fails >= self._cfg.quarantine_after_failures:
                # Periodic WARN so it stays visible in logs without
                # spamming every sweep.
                if self._stats.sweeps % self._cfg.quarantine_warn_every == 0:
                    logger.warning(
                        "redeemer: condition_id %s quarantined after %d "
                        "consecutive resolver failures — skipping. Manual "
                        "investigation required (likely neg-risk or "
                        "sub-cent market the resolver can't handle).",
                        cid, fails,
                    )
                return cid, None
            async with sem:
                try:
                    result = await self._resolver.fetch_resolution(cid)
                    # Reset the failure counter on any successful return
                    # (None is a valid "not yet resolved" result).
                    if cid in self._fail_counts:
                        del self._fail_counts[cid]
                    return cid, result
                except Exception:
                    self._fail_counts[cid] = fails + 1
                    if self._fail_counts[cid] == self._cfg.quarantine_after_failures:
                        logger.warning(
                            "redeemer: condition_id %s reached %d consecutive "
                            "resolver failures — QUARANTINING (will skip "
                            "future sweeps until restart or success)",
                            cid, self._fail_counts[cid],
                        )
                    else:
                        logger.exception(
                            "redeemer: resolver failed for %s (failure %d/%d)",
                            cid, self._fail_counts[cid],
                            self._cfg.quarantine_after_failures,
                        )
                    self._stats.errors += 1
                    return cid, None

        results = await asyncio.gather(
            *(_fetch(cid) for cid in by_market.keys())
        )
        market_state: dict[str, dict[str, Any] | None] = dict(results)

        pending = 0
        worthless_marked_this_sweep = 0
        redeemable_count = 0
        redeemable_usd = 0.0

        for cid, positions in by_market.items():
            market = market_state.get(cid)
            if market is None or not market.get("closed"):
                pending += len(positions)
                continue

            # Map token_id → outcome index → outcomePrice. Gamma
            # returns these as JSON-encoded strings, not lists.
            clob_token_ids = _coerce_list(market.get("clobTokenIds"))
            outcome_prices = _coerce_list(market.get("outcomePrices"))
            if (
                len(clob_token_ids) != len(outcome_prices)
                or not clob_token_ids
            ):
                logger.warning(
                    "redeemer: market %s closed but malformed "
                    "(clobTokenIds=%s, outcomePrices=%s); leaving as PENDING",
                    cid, clob_token_ids, outcome_prices,
                )
                pending += len(positions)
                continue

            for pos in positions:
                token_id = str(pos["token_id"])
                try:
                    idx = clob_token_ids.index(token_id)
                except ValueError:
                    # Token not in this market's outcome set — possibly
                    # stale data or we recorded the wrong condition_id.
                    # Mark to investigate, but don't auto-clear.
                    logger.warning(
                        "redeemer: position %s token_id %s not in market "
                        "%s outcomes %s; leaving as PENDING",
                        pos["position_id"], token_id, cid, clob_token_ids,
                    )
                    pending += 1
                    continue

                price_str = outcome_prices[idx].strip()
                position_id = int(pos["position_id"])
                is_paper = position_id in paper_position_ids
                # Resolved markets report "1" / "0" for the
                # winning/losing outcome respectively.
                if price_str in ("1", "1.0"):
                    payout_usd = float(pos["shares"])
                    if is_paper:
                        # Phase 33 — PAPER winning side: no on-chain
                        # tx to submit; truth-up realized_pnl with the
                        # PAPER sentinel.
                        ok = await self._mark_redeemed_and_clear_lock(
                            position_id=position_id,
                            redeemed_ts=now_ts,
                            redeem_tx_hash=PAPER_NO_TX,
                            payout_usd=payout_usd,
                        )
                        if ok:
                            paper_skipped_this_sweep += 1
                        continue
                    redeemable_count += 1
                    # Estimated payout: 1 share = $1 of pUSD on win.
                    redeemable_usd += payout_usd
                    # 2026-05-07 PHASE 16: auto-redeem path.
                    submitted = await self._maybe_auto_redeem(
                        pos=pos,
                        condition_id=cid,
                        outcome_index=idx,
                        neg_risk=bool(market.get("negRisk", False)),
                        market_label=str(market.get("question") or cid[:12]),
                        payout_usd=payout_usd,
                        now_ts=now_ts,
                    )
                    if submitted:
                        # Successful submission: row marked redeemed,
                        # don't keep counting it as REDEEMABLE.
                        redeemable_count -= 1
                        redeemable_usd -= payout_usd
                elif price_str in ("0", "0.0"):
                    if is_paper:
                        # Phase 33 — PAPER losing side: truth-up to
                        # −cost_basis via PAPER_NO_TX with payout=0.
                        ok = await self._mark_redeemed_and_clear_lock(
                            position_id=position_id,
                            redeemed_ts=now_ts,
                            redeem_tx_hash=PAPER_NO_TX,
                            payout_usd=0.0,
                        )
                        if ok:
                            paper_skipped_this_sweep += 1
                        continue
                    # 2026-05-07 PHASE 16: when auto-redeem is wired
                    # we still want to call redeem on the worthless
                    # side. The relayer covers gas, and clearing the
                    # on-chain inventory prevents PositionImporter
                    # from later phantom-importing zombie shares
                    # (Phase 14 settlement-window gate covers part
                    # of that, but resolved-NO inventory persists
                    # forever otherwise). The legacy
                    # WORTHLESS_NO_TX mark is the fallback when
                    # auto isn't enabled or the relayer fails.
                    submitted = await self._maybe_auto_redeem(
                        pos=pos,
                        condition_id=cid,
                        outcome_index=idx,
                        neg_risk=bool(market.get("negRisk", False)),
                        market_label=str(market.get("question") or cid[:12]),
                        payout_usd=0.0,
                        now_ts=now_ts,
                    )
                    if submitted:
                        worthless_marked_this_sweep += 1
                    else:
                        ok = await self._mark_redeemed_and_clear_lock(
                            position_id=int(pos["position_id"]),
                            redeemed_ts=now_ts,
                            redeem_tx_hash=WORTHLESS_NO_TX,
                        )
                        if ok:
                            worthless_marked_this_sweep += 1
                else:
                    # Resolution price between 0/1 (rare — refunded
                    # markets). Don't auto-clear; log for operator.
                    logger.warning(
                        "redeemer: position %s in market %s has "
                        "non-binary resolution price %r; leaving PENDING",
                        pos["position_id"], cid, price_str,
                    )
                    pending += 1

        self._stats.sweeps += 1
        self._stats.pending = pending
        self._stats.worthless_marked += worthless_marked_this_sweep
        self._stats.redeemable_count = redeemable_count
        self._stats.redeemable_usd = round(redeemable_usd, 4)
        self._stats.last_sweep_ts = now_ts
        # 2026-05-03 P2 #4: surface quarantine size for monitor.
        self._stats.quarantined_condition_ids = sum(
            1 for n in self._fail_counts.values()
            if n >= self._cfg.quarantine_after_failures
        )

        if (
            redeemable_usd >= self._cfg.nudge_threshold_usd
            and (now_ts - self._stats.last_nudge_ts) >= self._cfg.nudge_interval_s
        ):
            # Phase 16: nudge wording reflects whether auto-redeem is
            # wired/enabled. When auto is on, anything left over is
            # something the agent couldn't submit (retries exhausted,
            # neg-risk metadata missing, etc.) — operator should
            # investigate rather than simply "go click redeem."
            auto_state = self._auto_redeem_state_label()
            logger.warning(
                "redeemer: %d position(s) worth ~$%.2f are REDEEMABLE — "
                "auto=%s. Manual fallback: https://polymarket.com/portfolio",
                redeemable_count, redeemable_usd, auto_state,
            )
            self._stats.last_nudge_ts = now_ts

        return self._stats

    # ── Phase 16 (2026-05-07) — auto-redeem helper ─────────────────

    async def _maybe_auto_redeem(
        self,
        *,
        pos: dict[str, Any],
        condition_id: str,
        outcome_index: int,
        neg_risk: bool,
        market_label: str,
        payout_usd: float,
        now_ts: int,
    ) -> bool:
        """Submit a redemption via the relayer if auto is enabled and
        wired. Returns True iff the position was successfully marked
        redeemed (real on-chain or dry-run). False means the row
        stays in REDEEMABLE state for the next sweep.

        Failure modes:
          - Relayer raises → bump per-position retry counter; if at
            cap, log + leave as REDEEMABLE (manual fallback).
          - mark_redeemed returns False (race with another writer) →
            count it as already-handled, don't double-submit.
        """
        if self._relayer is None or not self._cfg.auto_enabled:
            return False
        position_id = int(pos["position_id"])
        retries = self._auto_retry_counts.get(position_id, 0)
        if retries >= self._cfg.auto_max_retries_per_position:
            # Phase 31 P1a — only warn once per position per process.
            if position_id not in self._warned_retry_cap:
                logger.warning(
                    "redeemer: position %s exceeded auto-redeem retry cap "
                    "(%d) — leaving as REDEEMABLE for manual claim "
                    "(condition_id=%s)",
                    position_id, retries, condition_id,
                )
                self._warned_retry_cap.add(position_id)
            # 2026-05-09 PHASE 32 — opt-in WORTHLESS_NO_TX escalation
            # for low-payout positions stuck at retry cap. Above the
            # ceiling we still leave the row REDEEMABLE for manual
            # recovery so we never auto-forfeit real money.
            if (
                self._cfg.worthless_no_tx_after_cap
                and float(payout_usd) <= float(
                    self._cfg.worthless_no_tx_payout_ceiling_usd
                )
            ):
                logger.critical(
                    "redeemer: position %s ESCALATED to WORTHLESS_NO_TX "
                    "after %d failed redeem attempts "
                    "(payout~=$%.2f <= ceiling $%.2f); truth-up will "
                    "record cost-basis loss",
                    position_id, retries, float(payout_usd),
                    float(self._cfg.worthless_no_tx_payout_ceiling_usd),
                )
                ok = await self._mark_redeemed_and_clear_lock(
                    position_id=position_id,
                    redeemed_ts=now_ts,
                    redeem_tx_hash=WORTHLESS_NO_TX,
                    payout_usd=0.0,
                )
                if ok:
                    self._stats.relayer_giveup_marks_worthless += 1
                    self._auto_retry_counts.pop(position_id, None)
                    return True
            # 2026-05-11 PHASE 36 — block-after-retry-cap fallback.
            # For positions NOT escalated to WORTHLESS (either because
            # the flag is off OR the payout exceeds the ceiling), mark
            # with RELAYER_BLOCKED to exit the sweep loop. On-chain
            # shares remain claimable via manual recovery.
            #
            # Critical: this branch does NOT pass payout_usd, so
            # PositionsRepo.mark_redeemed falls through to the simple
            # `SET redeemed_ts, redeem_tx_hash` branch — realized_pnl
            # and outcome are left untouched. The position retains its
            # original economic state until the operator chooses to
            # recover.
            if self._cfg.block_after_retry_cap:
                logger.critical(
                    "redeemer: position %s BLOCKED with RELAYER_BLOCKED "
                    "sentinel after %d failed redeem attempts "
                    "(payout~=$%.2f) — on-chain shares remain; manual "
                    "recovery required",
                    position_id, retries, float(payout_usd),
                )
                ok = await self._mark_redeemed_and_clear_lock(
                    position_id=position_id,
                    redeemed_ts=now_ts,
                    redeem_tx_hash=RELAYER_BLOCKED,
                )
                if ok:
                    self._stats.relayer_blocked += 1
                    self._auto_retry_counts.pop(position_id, None)
                    return True
            return False
        size = float(pos.get("shares") or 0.0)
        try:
            tx_hash = await self._relayer.submit_redeem(
                condition_id=condition_id,
                neg_risk=neg_risk,
                outcome_index=outcome_index,
                size=size,
                market_label=market_label,
            )
        except Exception:
            self._auto_retry_counts[position_id] = retries + 1
            self._stats.auto_redeem_errors += 1
            logger.exception(
                "redeemer: auto-redeem submit failed for position %s "
                "(condition_id=%s, retry %d/%d)",
                position_id, condition_id,
                self._auto_retry_counts[position_id],
                self._cfg.auto_max_retries_per_position,
            )
            return False
        ok = await self._mark_redeemed_and_clear_lock(
            position_id=position_id,
            redeemed_ts=now_ts,
            redeem_tx_hash=tx_hash,
            # Phase 31 P0c — pass payout so a real-tx-zero-payout redeem
            # (auto_redeem on a worthless side that the relayer still
            # submits successfully) gets the truth-up realized_pnl set.
            payout_usd=float(payout_usd or 0),
        )
        if not ok:
            # Either already redeemed (race) or DB write failed.
            # Don't double-count, but DO clear the retry counter so
            # next sweep can re-evaluate from scratch.
            self._auto_retry_counts.pop(position_id, None)
            return False
        self._stats.auto_redeemed += 1
        self._stats.auto_redeemed_usd = round(
            self._stats.auto_redeemed_usd + payout_usd, 4
        )
        self._auto_retry_counts.pop(position_id, None)
        logger.info(
            "redeemer: auto-redeemed position %s — payout≈$%.2f "
            "tx=%s (market=%s)",
            position_id, payout_usd, tx_hash, market_label,
        )
        return True

    def _auto_redeem_state_label(self) -> str:
        if self._relayer is None:
            return "DISABLED (no relayer wired)"
        if not self._cfg.auto_enabled:
            return "DISABLED (cfg.auto_enabled=False)"
        if getattr(self._relayer, "dry_run", False):
            return "DRY_RUN"
        return "ENABLED"


class GammaMarketResolver:
    """Default `_MarketResolver` — wraps Polymarket Gamma's `/markets`
    endpoint, returning the JSON row for a given `condition_id` only
    if `closed=true`. Returns None for not-yet-resolved markets.

    Built on `httpx` for sync transport reuse with the rest of the
    codebase. The session is kept alive across calls; close via the
    async context manager or `aclose()` on shutdown.
    """

    def __init__(
        self,
        base_url: str = "https://gamma-api.polymarket.com",
        timeout_s: float = 8.0,
    ) -> None:
        # Lazy-import httpx so unit tests that pass a fake resolver
        # don't pull the dep tree into the import graph.
        import httpx
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_s,
        )

    async def fetch_resolution(
        self, condition_id: str
    ) -> dict[str, Any] | None:
        # `closed=true` filters to resolved markets; if the market is
        # still open the response is empty and we return None
        # (treated as PENDING by the caller).
        resp = await self._client.get(
            "/markets",
            params={"condition_ids": condition_id, "closed": "true"},
        )
        if resp.status_code != 200:
            logger.warning(
                "redeemer: Gamma returned %s for %s",
                resp.status_code, condition_id,
            )
            return None
        items = resp.json()
        if not isinstance(items, list) or not items:
            return None
        # Defensive: confirm the returned row's conditionId actually
        # matches what we asked for. Gamma occasionally returns
        # paginated defaults when a filter is silently ignored.
        for m in items:
            if str(m.get("conditionId", "")).lower() == condition_id.lower():
                return m
        return None

    async def aclose(self) -> None:
        await self._client.aclose()
