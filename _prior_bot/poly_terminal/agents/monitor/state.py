"""Shared MonitorState passed to every route as a dependency.

Routes are stateless functions; they read from this state object.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from poly_terminal.agents.exit.agent import ExitAgent
    from poly_terminal.agents.freshness.agent import FreshnessTracker
    from poly_terminal.agents.inventory_reconciler.agent import (
        ReconciliationReport,
    )
    from poly_terminal.agents.redeemer.agent import RedeemerAgent
    from poly_terminal.agents.risk.agent import RiskAgent
    from poly_terminal.agents.wallet_intel.agent import WalletIntelAgent
    from poly_terminal.persistence.db import Database
    from poly_terminal.persistence.repositories.exit_evals import (
        ExitEvalsRepo,
    )
    from poly_terminal.persistence.repositories.fills import (
        FillsRepo,
        PositionsRepo,
    )
    from poly_terminal.persistence.repositories.gate_metrics import (
        GateMetricsRepo,
    )
    from poly_terminal.persistence.repositories.wallets import WalletsRepo


@dataclass
class MonitorState:
    """Bundle of every read-only handle the routes need.

    Optional fields tolerate partial wiring (useful in tests).
    """

    db: "Database | None" = None
    fills_repo: "FillsRepo | None" = None
    positions_repo: "PositionsRepo | None" = None
    wallets_repo: "WalletsRepo | None" = None
    gate_metrics_repo: "GateMetricsRepo | None" = None
    exit_evals_repo: "ExitEvalsRepo | None" = None
    wallet_agent: "WalletIntelAgent | None" = None
    risk_agent: "RiskAgent | None" = None
    exit_agent: "ExitAgent | None" = None
    redeemer_agent: "RedeemerAgent | None" = None
    inventory_report: "ReconciliationReport | None" = None
    # 2026-05-08 PHASE 30(b) — exit-path freshness tracker.
    # Snapshot exposed at /api/freshness for application-level
    # `live_canary_ready` rollup. None when not wired (tests / paper).
    freshness_tracker: "FreshnessTracker | None" = None
    config_fingerprint: str = ""
    bot_mode: str = "READ_ONLY"
    started_at: int = 0
    agent_heartbeat: dict[str, int] = field(default_factory=dict)
    strategy_intent_counts: dict[str, int] = field(default_factory=dict)
    latency_budgets: dict[str, Callable[[], dict]] = field(default_factory=dict)
