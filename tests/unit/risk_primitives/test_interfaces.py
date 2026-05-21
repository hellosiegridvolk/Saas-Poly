"""Risk primitives expose the right surface even before bodies are ported.

The bodies raise NotImplementedError until ``_prior_bot/`` lands; this
test asserts the interfaces are stable so the porting PR is a body-only
change.
"""

from __future__ import annotations

import inspect
from decimal import Decimal

import pytest

from shared.risk_primitives import (
    ExitReason,
    GammaSlugBuilder,
    PercentSLTP,
    PercentSLTPConfig,
    TickConfirmation,
    TickConfirmationConfig,
)


class TestTickConfirmation:
    def test_constructible(self) -> None:
        TickConfirmation(TickConfirmationConfig(confirm_ticks=3, max_age_seconds=5.0))

    def test_observe_not_implemented(self) -> None:
        primitive = TickConfirmation(
            TickConfirmationConfig(confirm_ticks=3, max_age_seconds=5.0)
        )
        with pytest.raises(NotImplementedError):
            primitive.observe(observation=None)  # type: ignore[arg-type]


class TestPercentSLTP:
    def test_constructible(self) -> None:
        PercentSLTP(
            PercentSLTPConfig(stop_loss_pct=Decimal("0.10"), take_profit_pct=Decimal("0.20"))
        )

    def test_exit_reasons_are_distinct(self) -> None:
        assert ExitReason.STOP_LOSS != ExitReason.TAKE_PROFIT


class TestGammaSlugBuilder:
    def test_build_signature_uses_unix_timestamp(self) -> None:
        sig = inspect.signature(GammaSlugBuilder.build)
        params = list(sig.parameters)
        assert "end_ts_unix" in params, (
            "GammaSlugBuilder.build must take end_ts_unix to enforce the "
            "unix-timestamp format (spec §16 operational lesson)."
        )

    def test_body_pending_port(self) -> None:
        with pytest.raises(NotImplementedError):
            GammaSlugBuilder().build("base", 1716230400)
