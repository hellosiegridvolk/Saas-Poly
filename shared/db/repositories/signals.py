from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.models import Signal
from shared.db.repositories.base import UserScopedRepository


class SignalRepository(UserScopedRepository[Signal]):
    model = Signal

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def list_for_instance(
        self, user_id: UUID, strategy_instance_id: UUID, limit: int = 100
    ) -> list[Signal]:
        stmt = (
            select(Signal)
            .where(
                Signal.user_id == user_id,
                Signal.strategy_instance_id == strategy_instance_id,
            )
            .order_by(Signal.emitted_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
