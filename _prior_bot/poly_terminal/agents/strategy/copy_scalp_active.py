"""CopyScalpActive — copy_scalp targeting the high-volume leaderboard cohort.

Behaviorally identical to CopyScalpStrategy; the only difference is the
strategy name ('copy_scalp_active') so fills and positions carry separate
attribution. This lets the operator run both the legacy WALLET_COPY_SCALP_OVERRIDE
cohort and the high-volume leaderboard cohort simultaneously and compare
their forward PnL independently in the scoreboard.

Wallet cohort: WALLET_COPY_SCALP_ACTIVE_OVERRIDE (comma-separated 0x addresses).
Exit config: reuses 'copy_scalp_active' entry in exit_config.EXIT_CONFIGS
             (identical params to copy_scalp — 7% SL / 5% TP / 10min hold).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from poly_terminal.agents.strategy.copy_scalp import CopyScalpConfig, CopyScalpStrategy


@dataclass(frozen=True)
class CopyScalpActiveConfig(CopyScalpConfig):
    """copy_scalp_active variant with wider slippage tolerance.

    High-volume leaderboard wallets trade fast-moving markets where the
    5% default cap rejects almost every signal (market reprices 20-30%
    within the ~3-13s polling pipeline). 20% allows entry while still
    blocking extreme latency outliers.
    """

    source_slippage_cap_pct: Decimal | None = Decimal("0.20")


class CopyScalpActiveStrategy(CopyScalpStrategy):
    """copy_scalp variant pointing at the high-volume leaderboard wallet set."""

    name = "copy_scalp_active"
