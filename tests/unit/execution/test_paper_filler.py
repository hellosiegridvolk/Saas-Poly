from __future__ import annotations

from decimal import Decimal

from services.execution.paper_filler import BookSnapshot, simulate_fill


def _book(**overrides) -> BookSnapshot:
    base = dict(
        best_bid=Decimal("0.498"),
        best_ask=Decimal("0.502"),
        bid_depth_at_best=Decimal("100"),
        ask_depth_at_best=Decimal("100"),
    )
    base.update(overrides)
    return BookSnapshot(**base)


class TestSimulateFill:
    def test_marketable_buy_fills_at_best_ask(self) -> None:
        result = simulate_fill(
            side="buy",
            size=Decimal("10"),
            limit_price=Decimal("0.510"),
            book=_book(),
        )
        assert result is not None
        assert result.fill_price == Decimal("0.502")
        assert result.filled_size == Decimal("10")

    def test_marketable_sell_fills_at_best_bid(self) -> None:
        result = simulate_fill(
            side="sell",
            size=Decimal("10"),
            limit_price=Decimal("0.490"),
            book=_book(),
        )
        assert result is not None
        assert result.fill_price == Decimal("0.498")

    def test_non_marketable_buy_returns_none(self) -> None:
        result = simulate_fill(
            side="buy",
            size=Decimal("10"),
            limit_price=Decimal("0.400"),
            book=_book(),
        )
        assert result is None

    def test_non_marketable_sell_returns_none(self) -> None:
        result = simulate_fill(
            side="sell",
            size=Decimal("10"),
            limit_price=Decimal("0.600"),
            book=_book(),
        )
        assert result is None

    def test_size_capped_by_depth(self) -> None:
        result = simulate_fill(
            side="buy",
            size=Decimal("500"),
            limit_price=Decimal("0.510"),
            book=_book(ask_depth_at_best=Decimal("100")),
        )
        assert result is not None
        assert result.filled_size == Decimal("100")

    def test_empty_opposite_side_returns_none(self) -> None:
        result = simulate_fill(
            side="buy",
            size=Decimal("10"),
            limit_price=Decimal("0.510"),
            book=_book(best_ask=None, ask_depth_at_best=Decimal("0")),
        )
        assert result is None

    def test_fee_is_round_down(self) -> None:
        # 10 * 0.5 * 0.02 = 0.1 exact; just check it never rounds up.
        result = simulate_fill(
            side="buy",
            size=Decimal("10"),
            limit_price=Decimal("0.500"),
            book=_book(best_ask=Decimal("0.500"), ask_depth_at_best=Decimal("100")),
            fee_pct=Decimal("0.02"),
        )
        assert result is not None
        assert result.fee <= Decimal("10") * result.fill_price * Decimal("0.02")
