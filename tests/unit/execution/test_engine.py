from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from services.execution.engine import PaperExecutionEngine
from services.execution.paper_filler import BookSnapshot
from shared.domain import Intent


def _book(**overrides) -> BookSnapshot:
    base = dict(
        best_bid=Decimal("0.498"),
        best_ask=Decimal("0.502"),
        bid_depth_at_best=Decimal("100"),
        ask_depth_at_best=Decimal("100"),
    )
    base.update(overrides)
    return BookSnapshot(**base)


def _intent(*, signal_id=None, side="buy", limit_price=Decimal("0.510")) -> Intent:
    return Intent(
        intent_id=uuid4(),
        signal_id=signal_id or uuid4(),
        user_id=uuid4(),
        strategy_id="ninety_cent",
        strategy_instance_id=uuid4(),
        market_id="0xmarket",
        token_id="0xtoken",
        side=side,
        size=Decimal("10"),
        limit_price=limit_price,
        time_in_force="FAK",
        risk_decisions=[],
        approved_at=datetime.now(tz=UTC),
    )


@pytest.fixture
def engine() -> PaperExecutionEngine:
    async def book_provider(market_id: str, token_id: str) -> BookSnapshot:
        return _book()

    return PaperExecutionEngine(book_provider=book_provider)


async def test_submit_returns_order_and_fill(engine: PaperExecutionEngine) -> None:
    result = await engine.submit(_intent())
    assert result is not None
    assert result.order.status == "submitted"
    assert result.fill is not None
    assert result.fill.size == Decimal("10")
    assert result.fill.price == Decimal("0.502")


async def test_idempotent_on_signal_id(engine: PaperExecutionEngine) -> None:
    intent = _intent()
    first = await engine.submit(intent)
    second = await engine.submit(intent)
    assert first is not None
    assert second is None


async def test_unmarketable_intent_returns_order_without_fill(
    engine: PaperExecutionEngine,
) -> None:
    intent = _intent(limit_price=Decimal("0.300"))
    result = await engine.submit(intent)
    assert result is not None
    assert result.fill is None


async def test_no_book_returns_order_without_fill() -> None:
    async def book_provider(market_id: str, token_id: str) -> BookSnapshot | None:
        return None

    engine = PaperExecutionEngine(book_provider=book_provider)
    result = await engine.submit(_intent())
    assert result is not None
    assert result.fill is None
