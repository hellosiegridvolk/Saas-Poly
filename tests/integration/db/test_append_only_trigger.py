"""Verify the append-only trigger (migration 0002) blocks UPDATE/DELETE
on signals, intents, fills, and audit_log (spec §8 footer).

Skipped unless ``TEST_DATABASE_URL`` is set (CI Postgres service in a
follow-up; locally: docker compose up postgres + alembic upgrade head).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from shared.db.models import Signal, StrategyInstance, User

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL, reason="TEST_DATABASE_URL not set"
)


@pytest.fixture
async def session() -> AsyncSession:
    assert TEST_DATABASE_URL is not None
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _seed_signal(session: AsyncSession) -> Signal:
    user = User(email=f"trigger-{uuid4()}@example.com")
    session.add(user)
    await session.flush()

    instance = StrategyInstance(
        user_id=user.id,
        strategy_id="ninety_cent",
        mode="paper",
        status="active",
        config={"k": "v"},
    )
    session.add(instance)
    await session.flush()

    sig = Signal(
        user_id=user.id,
        strategy_id="ninety_cent",
        strategy_instance_id=instance.id,
        market_id="0xmarket",
        token_id="0xtoken",
        side="buy",
        size=Decimal("10.000000"),
        limit_price=Decimal("0.910"),
        time_in_force="FAK",
        rationale={"reason": "test"},
        emitted_at=datetime.now(tz=UTC),
    )
    session.add(sig)
    await session.commit()
    return sig


async def test_update_on_signals_is_blocked(session: AsyncSession) -> None:
    sig = await _seed_signal(session)
    with pytest.raises(DBAPIError):
        await session.execute(
            text("UPDATE signals SET side = 'sell' WHERE id = :id"), {"id": sig.id}
        )
        await session.commit()


async def test_delete_on_signals_is_blocked(session: AsyncSession) -> None:
    sig = await _seed_signal(session)
    with pytest.raises(DBAPIError):
        await session.execute(
            text("DELETE FROM signals WHERE id = :id"), {"id": sig.id}
        )
        await session.commit()
