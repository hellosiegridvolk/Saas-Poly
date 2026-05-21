from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.models import User


class UserRepository:
    """Users are the root of the user_id chain; they don't take user_id themselves."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, user_id: UUID) -> User | None:
        stmt = select(User).where(User.id == user_id)
        result = await self._session.execute(stmt)
        user: User | None = result.scalar_one_or_none()
        return user

    async def get_by_email(self, email: str) -> User | None:
        stmt = select(User).where(User.email == email)
        result = await self._session.execute(stmt)
        user: User | None = result.scalar_one_or_none()
        return user

    async def add(self, user: User) -> User:
        self._session.add(user)
        await self._session.flush()
        return user
