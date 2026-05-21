from __future__ import annotations

from decimal import Decimal

import pytest

from services.reconciliation.position_math import (
    apply_fill_to_balance,
    apply_fill_to_position,
)


class TestApplyFillToPosition:
    def test_open_long_sets_avg_to_fill_price(self) -> None:
        u = apply_fill_to_position(
            prior_size=Decimal(0),
            prior_avg_cost=Decimal(0),
            fill_side="buy",
            fill_size=Decimal(10),
            fill_price=Decimal("0.500"),
        )
        assert u.new_size == Decimal(10)
        assert u.new_average_cost == Decimal("0.500")
        assert u.realized_pnl_delta == Decimal(0)

    def test_grow_long_cost_averages(self) -> None:
        u = apply_fill_to_position(
            prior_size=Decimal(10),
            prior_avg_cost=Decimal("0.400"),
            fill_side="buy",
            fill_size=Decimal(10),
            fill_price=Decimal("0.600"),
        )
        assert u.new_size == Decimal(20)
        assert u.new_average_cost == Decimal("0.5")
        assert u.realized_pnl_delta == Decimal(0)

    def test_partial_close_realizes_pnl(self) -> None:
        u = apply_fill_to_position(
            prior_size=Decimal(10),
            prior_avg_cost=Decimal("0.400"),
            fill_side="sell",
            fill_size=Decimal(4),
            fill_price=Decimal("0.500"),
        )
        assert u.new_size == Decimal(6)
        assert u.new_average_cost == Decimal("0.400")
        assert u.realized_pnl_delta == Decimal("0.4")  # 4 * (0.5 - 0.4)

    def test_full_close_realizes_full_pnl(self) -> None:
        u = apply_fill_to_position(
            prior_size=Decimal(10),
            prior_avg_cost=Decimal("0.400"),
            fill_side="sell",
            fill_size=Decimal(10),
            fill_price=Decimal("0.600"),
        )
        assert u.new_size == Decimal(0)
        assert u.new_average_cost == Decimal(0)
        assert u.realized_pnl_delta == Decimal("2")  # 10 * (0.6 - 0.4)

    def test_close_with_loss_yields_negative_realized(self) -> None:
        u = apply_fill_to_position(
            prior_size=Decimal(10),
            prior_avg_cost=Decimal("0.500"),
            fill_side="sell",
            fill_size=Decimal(10),
            fill_price=Decimal("0.300"),
        )
        assert u.realized_pnl_delta == Decimal("-2")

    def test_flip_from_long_to_short(self) -> None:
        u = apply_fill_to_position(
            prior_size=Decimal(10),
            prior_avg_cost=Decimal("0.400"),
            fill_side="sell",
            fill_size=Decimal(15),
            fill_price=Decimal("0.500"),
        )
        assert u.new_size == Decimal(-5)
        assert u.new_average_cost == Decimal("0.500")
        # closed 10 longs at +0.1 each = 1.0
        assert u.realized_pnl_delta == Decimal("1.0")

    def test_zero_fill_size_rejected(self) -> None:
        with pytest.raises(ValueError):
            apply_fill_to_position(
                prior_size=Decimal(0),
                prior_avg_cost=Decimal(0),
                fill_side="buy",
                fill_size=Decimal(0),
                fill_price=Decimal("0.5"),
            )


class TestApplyFillToBalance:
    def test_buy_debits_notional_and_fee(self) -> None:
        new = apply_fill_to_balance(
            prior_balance=Decimal(100),
            fill_side="buy",
            fill_size=Decimal(10),
            fill_price=Decimal("0.500"),
            fee=Decimal("0.10"),
        )
        assert new == Decimal("94.9")

    def test_sell_credits_notional_minus_fee(self) -> None:
        new = apply_fill_to_balance(
            prior_balance=Decimal(100),
            fill_side="sell",
            fill_size=Decimal(10),
            fill_price=Decimal("0.500"),
            fee=Decimal("0.10"),
        )
        assert new == Decimal("104.9")
