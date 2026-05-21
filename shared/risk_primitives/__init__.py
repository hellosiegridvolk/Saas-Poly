"""Cross-cutting risk primitives shared by strategies and the risk engine
(spec §10 gate 24, §12.2).

Interfaces only in PR A. Algorithm bodies are ported from the prior bot
in a follow-up — see ``_prior_bot/README.md``.
"""

from shared.risk_primitives.gamma_slug_builder import GammaSlugBuilder
from shared.risk_primitives.percent_sltp import (
    ExitDecision,
    ExitReason,
    PercentSLTP,
    PercentSLTPConfig,
)
from shared.risk_primitives.tick_confirmation import (
    PriceObservation,
    TickConfirmation,
    TickConfirmationConfig,
)

__all__ = [
    "ExitDecision",
    "ExitReason",
    "GammaSlugBuilder",
    "PercentSLTP",
    "PercentSLTPConfig",
    "PriceObservation",
    "TickConfirmation",
    "TickConfirmationConfig",
]
