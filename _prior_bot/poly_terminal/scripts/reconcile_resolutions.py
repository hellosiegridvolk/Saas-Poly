"""Standalone CLI: reconcile flat-closed positions against Gamma resolution.

2026-05-05 — sweeps every closed position with `realized_pnl=0` AND
`exit_price = entry_price` (the shadow-price-fallback fingerprint),
groups by market_id, queries Gamma `/markets?condition_ids=…&closed=true`,
and rewrites realized_pnl from the resolved `outcomePrices`. Idempotent;
already-reconciled rows (`outcome='TIME_RECONCILED'`) are skipped.

Usage:

    poly-resolution-reconcile                     # one pass, exit
    poly-resolution-reconcile --loop --interval 600   # daemon, every 10 min
    poly-resolution-reconcile --dry-run           # preview, don't write
    poly-resolution-reconcile --limit 100         # cap candidates per pass

Environment:
    DB_PATH                  sqlite path (default: exports/state.db)
    GAMMA_API_URL            override (default: https://gamma-api.polymarket.com)
    LOG_LEVEL                DEBUG|INFO|WARNING|ERROR (default INFO)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from poly_terminal.agents.redeemer.agent import GammaMarketResolver
from poly_terminal.persistence.db import Database
from poly_terminal.persistence.repositories.fills import PositionsRepo
from poly_terminal.research.resolution_reconciler import (
    ReconcileConfig,
    ReconcileStats,
    ResolutionReconciler,
    run_loop,
)

logger = logging.getLogger("poly_terminal.scripts.reconcile_resolutions")

_DEFAULT_DB_PATH = "exports/state.db"
_DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"


def _setup_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _log_stats(stats: ReconcileStats) -> None:
    logger.info(
        "reconcile pass: candidates=%d  win=%d loss=%d  pending=%d "
        "missing=%d malformed=%d  not_in_market=%d refund=%d  "
        "no_op=%d  errors=%d  pnl_correction=$%+.2f  markets=%d",
        stats.candidates,
        stats.reconciled_win,
        stats.reconciled_loss,
        stats.market_pending,
        stats.market_missing,
        stats.market_malformed,
        stats.token_not_in_market,
        stats.refund_or_invalid,
        stats.update_no_op,
        stats.errors,
        stats.pnl_corrected_usd,
        len(stats.markets_queried),
    )


async def _main() -> int:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0] if __doc__ else None,
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously, polling every --interval seconds.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=600.0,
        help="Loop interval in seconds (default 600 = 10min).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute what would change but do NOT write.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap candidates per pass (default: no limit).",
    )
    args = parser.parse_args()

    db_path = os.environ.get("DB_PATH", "").strip() or _DEFAULT_DB_PATH
    gamma_url = (
        os.environ.get("GAMMA_API_URL", "").strip() or _DEFAULT_GAMMA_URL
    )

    db = Database(db_path)
    applied = await db.initialize()
    logger.info(
        "reconcile_resolutions: db=%s migrations_applied=%d gamma=%s "
        "loop=%s interval=%.0fs dry_run=%s limit=%s",
        db_path, applied, gamma_url, args.loop, args.interval,
        args.dry_run, args.limit,
    )

    repo = PositionsRepo(db)
    if args.dry_run:
        # Wrap the real repo so update_reconciled_pnl is a no-op while
        # we still log what WOULD have been written.
        class _DryRunRepo:
            def __init__(self, inner: PositionsRepo) -> None:
                self._inner = inner

            async def fetch_unreconciled_flat_closes(
                self, *, limit: int | None = None
            ):
                return await self._inner.fetch_unreconciled_flat_closes(
                    limit=limit,
                )

            async def update_reconciled_pnl(self, **kwargs) -> bool:
                logger.info(
                    "[dry-run] would update pid=%s exit=%s pnl=%+.4f outcome=%s",
                    kwargs.get("position_id"),
                    kwargs.get("exit_price"),
                    float(kwargs.get("realized_pnl", 0.0)),
                    kwargs.get("outcome"),
                )
                return True

        wrapped_repo = _DryRunRepo(repo)
    else:
        wrapped_repo = repo  # type: ignore[assignment]

    resolver = GammaMarketResolver(base_url=gamma_url)
    cfg = ReconcileConfig(
        candidates_per_pass=args.limit if args.limit and args.limit > 0 else None,
    )
    reconciler = ResolutionReconciler(
        positions_repo=wrapped_repo,  # type: ignore[arg-type]
        market_resolver=resolver,
        cfg=cfg,
    )

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum: int) -> None:
        logger.info("reconcile_resolutions: signal %s received", signum)
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, int(sig))
        except (NotImplementedError, RuntimeError):
            pass

    try:
        if args.loop:
            await run_loop(
                reconciler,
                interval_s=args.interval,
                shutdown=shutdown,
                on_stats=_log_stats,
            )
        else:
            stats = await reconciler.reconcile_once()
            _log_stats(stats)
    finally:
        await resolver.aclose()

    return 0


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(main())
