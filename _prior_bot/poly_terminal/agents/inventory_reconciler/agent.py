"""InventoryReconcilerAgent — DB ↔ on-chain CTF balanceOf gate.

Runs at LIVE / LIVE_DRY / CLOSE_ONLY startup. For every position with
`closed_ts IS NULL`, queries the on-chain ERC1155 balance for
(proxy_wallet, token_id) and compares to the DB-stored shares.

Behaviour:
  - Hard fail (raises InventoryDriftError) if any per-position drift
    exceeds the configured threshold.
  - Soft mode (`hard_gate=False`) logs the report and returns it
    without raising — used in PAPER + by tests that want to see the
    drift counts without aborting boot.
  - Multiple positions on the same token are summed before comparison
    against on-chain balance — on-chain is per-(owner, token_id), the
    DB carries one row per fill so the same token can appear N times.
  - Soft tolerance: drift ≤ 1 share AND ≤ 1% of DB shares is treated
    as rounding noise (CTF wei-level precision vs DB float storage).
    Tunable via `tolerance_shares` and `tolerance_pct`.

The agent is intentionally synchronous-on-network — it makes one HTTP
call per unique held token at startup, runs once, and exits. The cost
of a Web3 library + async HTTP for one shot of work is not justified.
Network calls run via `asyncio.to_thread` so the boot loop doesn't
block.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from poly_terminal.agents.inventory_reconciler.ctf_reader import (
    CTFBalanceReader,
    CTFReadError,
)

if TYPE_CHECKING:
    from poly_terminal.persistence.repositories.fills import PositionsRepo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Mismatch:
    """One token where DB-aggregated shares disagree with on-chain."""

    token_id: str
    db_shares: float
    on_chain_shares: float
    drift_shares: float           # signed: positive = DB over-counts
    drift_pct: float              # |drift| / max(db, on_chain)
    position_ids: tuple[int, ...] # which DB rows contributed to db_shares


@dataclass
class ReconciliationReport:
    """Outcome of one reconciliation pass."""

    started_at: int
    finished_at: int
    proxy_wallet: str
    db_open_positions: int
    db_unique_tokens: int
    on_chain_calls: int
    failed_calls: int
    matches: int
    mismatches: list[Mismatch] = field(default_factory=list)
    soft_skipped: list[Mismatch] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def hard_drift_count(self) -> int:
        return len(self.mismatches)

    def summary(self) -> str:
        return (
            f"InventoryReconciler: open={self.db_open_positions} "
            f"tokens={self.db_unique_tokens} "
            f"on_chain_calls={self.on_chain_calls} "
            f"failed={self.failed_calls} "
            f"matches={self.matches} "
            f"hard_mismatches={len(self.mismatches)} "
            f"soft_skipped={len(self.soft_skipped)}"
        )


class InventoryDriftError(RuntimeError):
    """Raised by `run()` when any hard mismatch was detected and
    hard_gate=True. The error message includes the report summary; the
    full report is attached as `.report` for the operator log."""

    def __init__(self, report: ReconciliationReport, message: str) -> None:
        super().__init__(message)
        self.report = report


@dataclass(frozen=True)
class InventoryReconcilerConfig:
    proxy_wallet: str
    tolerance_shares: float = 1.0       # absolute floor below which drift is noise
    tolerance_pct: float = 0.01         # also requires < 1% pct drift
    hard_gate: bool = True              # raise on mismatch (LIVE/LIVE_DRY)
    fail_open_on_rpc_error: bool = False
    """When True, RPC failures don't block boot. Use ONLY in tightly
    controlled environments (e.g. tests, dev with no internet).
    Production must keep False so a degraded RPC doesn't silently let
    inventory drift through."""


class InventoryReconcilerAgent:
    def __init__(
        self,
        cfg: InventoryReconcilerConfig,
        positions_repo: "PositionsRepo",
        ctf_reader: CTFBalanceReader,
    ) -> None:
        if not cfg.proxy_wallet or not cfg.proxy_wallet.startswith("0x"):
            raise ValueError(
                f"invalid proxy_wallet: {cfg.proxy_wallet!r} "
                "(must be 0x-prefixed hex)"
            )
        self._cfg = cfg
        self._positions = positions_repo
        self._reader = ctf_reader
        self._last_report: ReconciliationReport | None = None

    @property
    def last_report(self) -> ReconciliationReport | None:
        return self._last_report

    async def run(self) -> ReconciliationReport:
        """Reconcile DB open positions vs on-chain balances. Raises
        InventoryDriftError if hard_gate and any mismatch was found."""
        started = int(time.time())
        rows = await self._positions.fetch_all_open()

        # Aggregate DB shares per token_id (multiple positions on same
        # token sum up — on-chain is per-(owner, token_id)).
        per_token_shares: dict[str, float] = {}
        per_token_pids: dict[str, list[int]] = {}
        for r in rows:
            token_id = str(r["token_id"])
            shares = float(r["shares"])
            pid = int(r["position_id"])
            per_token_shares[token_id] = per_token_shares.get(token_id, 0.0) + shares
            per_token_pids.setdefault(token_id, []).append(pid)

        report = ReconciliationReport(
            started_at=started,
            finished_at=0,
            proxy_wallet=self._cfg.proxy_wallet,
            db_open_positions=len(rows),
            db_unique_tokens=len(per_token_shares),
            on_chain_calls=0,
            failed_calls=0,
            matches=0,
        )

        for token_id, db_shares in per_token_shares.items():
            try:
                on_chain = await asyncio.to_thread(
                    self._reader.shares_of,
                    self._cfg.proxy_wallet,
                    token_id,
                )
            except CTFReadError as exc:
                report.failed_calls += 1
                report.notes.append(
                    f"rpc_failed token={token_id[:12]}…: {exc}"
                )
                if not self._cfg.fail_open_on_rpc_error:
                    # Treat as a hard mismatch — RPC failure during
                    # the safety gate is itself a reason to refuse boot.
                    report.mismatches.append(
                        Mismatch(
                            token_id=token_id,
                            db_shares=db_shares,
                            on_chain_shares=float("nan"),
                            drift_shares=float("nan"),
                            drift_pct=float("nan"),
                            position_ids=tuple(per_token_pids[token_id]),
                        )
                    )
                continue
            report.on_chain_calls += 1

            drift_shares = db_shares - on_chain
            denom = max(abs(db_shares), abs(on_chain), 1e-12)
            drift_pct = abs(drift_shares) / denom
            mismatch = Mismatch(
                token_id=token_id,
                db_shares=db_shares,
                on_chain_shares=on_chain,
                drift_shares=drift_shares,
                drift_pct=drift_pct,
                position_ids=tuple(per_token_pids[token_id]),
            )
            if (
                abs(drift_shares) <= self._cfg.tolerance_shares
                and drift_pct <= self._cfg.tolerance_pct
            ):
                # Within rounding tolerance — count as match for stats
                # but record under soft_skipped so operators still see
                # which tokens are noisy.
                report.matches += 1
                if drift_shares != 0:
                    report.soft_skipped.append(mismatch)
                continue
            report.mismatches.append(mismatch)

        report.finished_at = int(time.time())
        self._last_report = report
        logger.info(report.summary())
        for m in report.mismatches:
            logger.warning(
                "inventory_drift token=%s db=%.4f on_chain=%.4f "
                "drift=%.4f (%.2f%%) pids=%s",
                m.token_id[:16] + "…",
                m.db_shares,
                m.on_chain_shares,
                m.drift_shares,
                m.drift_pct * 100,
                m.position_ids,
            )
        if report.mismatches and self._cfg.hard_gate:
            raise InventoryDriftError(
                report,
                f"hard inventory drift: {len(report.mismatches)} "
                f"token(s) disagree with on-chain — refusing to start. "
                f"See last_report for details.",
            )
        return report
