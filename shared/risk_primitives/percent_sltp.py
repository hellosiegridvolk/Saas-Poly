"""PercentSLTP — percent-based stop-loss / take-profit (spec §10 gate 24,
§12.2 ``certainty_farm`` / ``ninety_cent`` exits).

Public interface only. Algorithm body ported from ``_prior_bot/`` in a
follow-up PR.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from shared.domain import Position


class ExitReason(StrEnum):
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"


@dataclass(frozen=True)
class PercentSLTPConfig:
    stop_loss_pct: Decimal
    """Loss percentage that triggers exit; positive number e.g. 0.10 = 10%."""
    take_profit_pct: Decimal
    """Gain percentage that triggers exit; positive number e.g. 0.20 = 20%."""


@dataclass(frozen=True)
class ExitDecision:
    reason: ExitReason
    triggered_at_price: Decimal
    pnl_pct: Decimal


class PercentSLTP:
    def __init__(self, config: PercentSLTPConfig) -> None:
        self._config = config

    def evaluate(self, position: Position, current_price: Decimal) -> ExitDecision | None:
        """Return an ExitDecision if SL or TP is tripped, else None."""
        raise NotImplementedError("port from _prior_bot/")
