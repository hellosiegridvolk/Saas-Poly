"""LedgerSnapshotRefresher — closes the no-op allocator-gate hole.

Phase 34 (2026-05-11) — companion fix to the wiring debt flagged in
deep-research-report (30). Before this module landed, every strategy
in `main.py` wired:

    ledger_snapshot_getter=lambda: LedgerSnapshot()

…which always returned an EMPTY snapshot (no open positions, $0
realized PnL, no quarantined tokens). That made 3-5 of the 8
allocator gates effectively no-ops:

  * position limit gate    (no open_positions → never hits the cap)
  * exposure limit gate    (sum=0 → never hits the cap)
  * one-strategy gate      (no open_positions → never sees the conflict)
  * daily-loss gate        (realized_today=0 → never blocks)
  * quarantine lock gate   (empty set → never blocks)

Strategies are synchronous (they call `ledger_snapshot_getter()`
inline), but `PositionsRepo.fetch_all_open()` is async. This module
bridges the gap by maintaining a *cached* snapshot that gets refreshed
on a background asyncio task. The `snapshot()` getter is synchronous
and returns the most recent cached value — what every strategy now
calls instead of `lambda: LedgerSnapshot()`.

Refresh cadence default 15s — short enough that a position closing or
opening rapidly is reflected before the next intent is evaluated, long
enough that the SQLite read pressure stays negligible (≤4 reads/min).

Resilience: if a refresh errors, the loop logs and keeps ticking with
the prior cached snapshot. We do NOT zero the cache on error — that
would silently re-open the no-op gates.

Lifecycle:
  refresher = LedgerSnapshotRefresher(positions_repo=repo)
  await refresher.start()         # primes cache + spawns loop
  ...
  snap = refresher.snapshot()     # synchronous; safe in strategy code
  ...
  await refresher.stop()          # cancels loop cleanly
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Callable

from poly_terminal.agents.strategy.allocator import (
    LedgerSnapshot,
    OpenPosition,
)

if TYPE_CHECKING:  # pragma: no cover — typing only
    from poly_terminal.persistence.repositories.fills import PositionsRepo


logger = logging.getLogger(__name__)


# Default refresh cadence. Picked at 15s because:
#  * Strategies emit intents at most ~once per few seconds, so a
#    15s-old snapshot is at worst one decision cycle stale.
#  * SQLite read at this rate is ~4 reads/min, well under any
#    contention threshold even on the bot's loaded paths.
_DEFAULT_REFRESH_INTERVAL_S = 15.0

# Default daily window for realized_pnl. 86400s = 24h rolling window
# matches DAILY_LOSS_CAP_USD semantics in the allocator.
_DEFAULT_DAY_WINDOW_S = 86_400


class LedgerSnapshotRefresher:
    """Background-refreshed cache of a LedgerSnapshot.

    Public surface kept deliberately small:
      * snapshot()       — sync, returns the latest cached value
      * refresh_once()   — async, forces a single refresh (for tests)
      * start() / stop() — lifecycle for the background loop
    """

    def __init__(
        self,
        *,
        positions_repo: "PositionsRepo",
        refresh_interval_s: float = _DEFAULT_REFRESH_INTERVAL_S,
        day_window_s: int = _DEFAULT_DAY_WINDOW_S,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._repo = positions_repo
        self._interval = float(refresh_interval_s)
        self._day_window_s = int(day_window_s)
        self._clock = clock
        # Safe empty default — readers that fire before the first
        # refresh land see a closed-allocator default (no positions,
        # zero PnL), which fails open the same way the old empty
        # LedgerSnapshot() did. Once start() runs the first refresh,
        # this gets replaced with the real state.
        self._cached: LedgerSnapshot = LedgerSnapshot()
        self._task: asyncio.Task | None = None
        self._stopping = False

    # ---------------------------------------------------- public API
    def snapshot(self) -> LedgerSnapshot:
        """Return the latest cached snapshot. Synchronous + thread-safe
        because LedgerSnapshot is frozen and only assigned atomically."""
        return self._cached

    async def refresh_once(self) -> LedgerSnapshot:
        """Pull live state from the repo into a new snapshot and cache it.

        Returns the new snapshot. Raises if the repo raises — the
        background loop catches and logs; direct callers (tests, manual
        refresh) get the raw exception so failures surface."""
        open_rows = await self._repo.fetch_all_open()
        open_positions = tuple(
            OpenPosition(
                position_id=int(row["position_id"]),
                strategy_name=str(row.get("strategy", "") or ""),
                token_id=str(row["token_id"]),
                cost_basis_usd=float(row["cost_basis_usd"]),
                source_wallet=str(row["source_wallet"]) if row.get("source_wallet") else None,
            )
            for row in open_rows
        )
        since_ts = int(self._clock()) - self._day_window_s
        # realized_pnl_since returns (count, sum); we only need the sum.
        _count, realized_today = await self._repo.realized_pnl_since(since_ts)
        new_snap = LedgerSnapshot(
            open_positions=open_positions,
            realized_today_usd=float(realized_today),
            # quarantine machinery not yet wired into a producer;
            # keep an empty set so the gate behaves identically to
            # the pre-fix state for this dimension. See playbook §13.
            quarantined_tokens=frozenset(),
        )
        self._cached = new_snap
        return new_snap

    async def start(self) -> None:
        """Prime the cache with one immediate refresh, then spawn the
        background loop. Safe to call multiple times — second call is
        a no-op if the loop is already running."""
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        # Prime the cache once synchronously so the FIRST strategy
        # call after start() reads real data, not the empty default.
        try:
            await self.refresh_once()
        except Exception as exc:  # noqa: BLE001 — broad log + continue
            logger.warning(
                "ledger_refresher: initial refresh failed: %s", exc,
            )
        self._task = asyncio.create_task(
            self._loop(), name="ledger_refresher",
        )

    async def stop(self) -> None:
        """Cancel the background loop. Safe even if start() was never
        called. Idempotent."""
        self._stopping = True
        task = self._task
        if task is None:
            return
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "ledger_refresher: stop() — loop exit error: %s", exc,
                )
        self._task = None

    # ---------------------------------------------------- internal
    async def _loop(self) -> None:
        """Refresh every `interval` seconds until stop() flips the flag.

        Exceptions are caught and logged; the loop continues with the
        prior cached snapshot. Zeroing the cache on error would silently
        re-open the no-op gates, which is the whole bug we're closing."""
        while not self._stopping:
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "ledger_refresher: refresh failed (keeping prior "
                    "cache): %s",
                    exc,
                )
            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                raise
