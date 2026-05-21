from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from services.reconciliation.engine import ReconciliationEngine
from shared.domain import Fill


@dataclass
class FakePositions:
    store: dict[tuple[UUID, str, str], tuple[Decimal, Decimal, Decimal]] = field(
        default_factory=dict
    )

    async def get(
        self, user_id: UUID, market_id: str, token_id: str
    ) -> tuple[Decimal, Decimal, Decimal] | None:
        return self.store.get((user_id, market_id, token_id))

    async def upsert(
        self,
        *,
        user_id: UUID,
        market_id: str,
        token_id: str,
        size: Decimal,
        average_cost: Decimal,
        realized_pnl: Decimal,
    ) -> None:
        self.store[(user_id, market_id, token_id)] = (size, average_cost, realized_pnl)


@dataclass
class FakeBalances:
    store: dict[tuple[UUID, str], Decimal] = field(default_factory=dict)

    async def get(self, user_id: UUID, asset: str) -> Decimal:
        return self.store.get((user_id, asset), Decimal(1000))

    async def upsert(self, user_id: UUID, asset: str, balance: Decimal) -> None:
        self.store[(user_id, asset)] = balance


def _fill(
    *, user_id: UUID | None = None, side="buy", size=Decimal(10), price=Decimal("0.500"),
    fee=Decimal("0.10")
) -> Fill:
    return Fill(
        fill_id=str(uuid4()),
        order_id=str(uuid4()),
        intent_id=uuid4(),
        user_id=user_id or uuid4(),
        market_id="0xmarket",
        token_id="0xtoken",
        side=side,
        size=size,
        price=price,
        fee=fee,
        filled_at=datetime.now(tz=UTC),
    )


@pytest.fixture
def engine() -> tuple[ReconciliationEngine, FakePositions, FakeBalances]:
    pos = FakePositions()
    bal = FakeBalances()
    return ReconciliationEngine(pos, bal), pos, bal


async def test_first_buy_opens_long_and_debits_balance(
    engine: tuple[ReconciliationEngine, FakePositions, FakeBalances],
) -> None:
    e, pos, bal = engine
    fill = _fill()
    await e.apply(fill)
    assert pos.store[(fill.user_id, "0xmarket", "0xtoken")] == (
        Decimal(10),
        Decimal("0.500"),
        Decimal(0),
    )
    assert bal.store[(fill.user_id, "USDC")] == Decimal("994.9")


async def test_sequential_buys_average_cost(
    engine: tuple[ReconciliationEngine, FakePositions, FakeBalances],
) -> None:
    e, pos, _ = engine
    user = uuid4()
    await e.apply(_fill(user_id=user, size=Decimal(10), price=Decimal("0.400"), fee=Decimal(0)))
    await e.apply(_fill(user_id=user, size=Decimal(10), price=Decimal("0.600"), fee=Decimal(0)))
    size, avg, realized = pos.store[(user, "0xmarket", "0xtoken")]
    assert size == Decimal(20)
    assert avg == Decimal("0.5")
    assert realized == Decimal(0)


async def test_close_realizes_pnl(
    engine: tuple[ReconciliationEngine, FakePositions, FakeBalances],
) -> None:
    e, pos, _ = engine
    user = uuid4()
    await e.apply(
        _fill(user_id=user, side="buy", size=Decimal(10), price=Decimal("0.400"), fee=Decimal(0))
    )
    await e.apply(
        _fill(user_id=user, side="sell", size=Decimal(10), price=Decimal("0.600"), fee=Decimal(0))
    )
    size, _avg, realized = pos.store[(user, "0xmarket", "0xtoken")]
    assert size == Decimal(0)
    assert realized == Decimal(2)
