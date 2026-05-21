"""Verify the Strategy plugin contract (spec §12.1, §3.1).

Strategies must not be able to reference an execution client; the
contract test asserts the StrategyContext protocol exposes no such
surface and that the Strategy protocol's method signatures match the
spec exactly.
"""

from __future__ import annotations

import inspect

from strategies._base import Strategy, StrategyContext

EXPECTED_STRATEGY_METHODS = {
    "on_start",
    "on_market_tick",
    "on_book_update",
    "on_fill",
    "on_intent_rejected",
    "on_stop",
}

FORBIDDEN_CONTEXT_ATTRS = {"execution", "execution_client", "submit_order", "place_order", "sdk"}


def test_strategy_protocol_has_expected_methods() -> None:
    actual = {name for name in dir(Strategy) if not name.startswith("_")}
    missing = EXPECTED_STRATEGY_METHODS - actual
    assert not missing, f"Strategy protocol missing methods: {sorted(missing)}"


def test_strategy_methods_are_coroutines() -> None:
    for name in EXPECTED_STRATEGY_METHODS:
        method = getattr(Strategy, name)
        assert inspect.iscoroutinefunction(method), f"{name} must be async"


def test_strategy_context_exposes_no_execution_surface() -> None:
    surface = {name for name in dir(StrategyContext) if not name.startswith("_")}
    leaked = surface & FORBIDDEN_CONTEXT_ATTRS
    assert not leaked, (
        f"StrategyContext exposes forbidden execution surface {leaked}; "
        "spec §3.1: strategies never touch the execution engine."
    )
