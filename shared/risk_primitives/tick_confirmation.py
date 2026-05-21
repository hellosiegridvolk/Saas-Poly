"""TickConfirmation — confirms that an exit signal holds across N ticks
before firing, to avoid acting on single-tick noise (spec §10 gate 24).

Public interface only. The algorithm is ported from
``_prior_bot/`` and wired in a follow-up PR.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class TickConfirmationConfig:
    confirm_ticks: int
    """How many consecutive ticks must agree before confirmation."""
    max_age_seconds: float
    """Discard observations older than this when evaluating."""


@dataclass(frozen=True)
class PriceObservation:
    price: Decimal
    observed_at: datetime


class TickConfirmation:
    """Streaming confirmer. Feed observations in order; ask :meth:`confirmed`."""

    def __init__(self, config: TickConfirmationConfig) -> None:
        self._config = config

    def observe(self, observation: PriceObservation) -> None:
        raise NotImplementedError("port from _prior_bot/")

    def confirmed(self, predicate: object) -> bool:
        """Return True iff the predicate has held for ``confirm_ticks`` in a row."""
        raise NotImplementedError("port from _prior_bot/")

    def reset(self) -> None:
        raise NotImplementedError("port from _prior_bot/")
