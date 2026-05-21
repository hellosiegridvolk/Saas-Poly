from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.models import StrategyInstance
from shared.db.repositories.base import UserScopedRepository


class StrategyInstanceRepository(UserScopedRepository[StrategyInstance]):
    model = StrategyInstance

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def list_active(self, user_id: UUID) -> list[StrategyInstance]:
        stmt = select(StrategyInstance).where(
            StrategyInstance.user_id == user_id,
            StrategyInstance.status == "active",
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
