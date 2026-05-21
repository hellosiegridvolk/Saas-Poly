"""Repository base. Enforces the multi-tenancy invariant (spec §3.6):
every query that touches a user-scoped row is filtered by user_id.

Subclasses MUST:
  - declare ``model`` (the SQLAlchemy mapped class)
  - take ``user_id`` as the first non-self argument of every query method

The test suite (``tests/unit/db/test_repositories.py``) asserts the
contract by introspecting subclass method signatures. Skipping user_id
fails CI.
"""

from __future__ import annotations

from abc import ABC
from typing import ClassVar, Generic, TypeVar, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.base import Base

ModelT = TypeVar("ModelT", bound=Base)


class UserScopedRepository(Generic[ModelT], ABC):
    """Base for any repository over a table that carries user_id."""

    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, user_id: UUID, entity_id: UUID) -> ModelT | None:
        # `model` is statically a `type[Base]`; concrete subclasses (enforced by
        # tests/unit/db/test_repositories_contract.py) carry `id` and `user_id` columns.
        stmt = select(self.model).where(
            self.model.id == entity_id,  # type: ignore[attr-defined]
            self.model.user_id == user_id,  # type: ignore[attr-defined]
        )
        result = await self._session.execute(stmt)
        return cast("ModelT | None", result.scalar_one_or_none())

    async def list(self, user_id: UUID, limit: int = 100) -> list[ModelT]:
        stmt = (
            select(self.model)
            .where(self.model.user_id == user_id)  # type: ignore[attr-defined]
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return cast("list[ModelT]", list(result.scalars().all()))

    async def add(self, entity: ModelT) -> ModelT:
        self._session.add(entity)
        await self._session.flush()
        return entity
