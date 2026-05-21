"""Reconciliation engine (spec §3.5, §15 Phase 1 step 7).

In paper mode, the reconciler reads ``fill.received`` events and updates
positions + balances. It is the single source of truth for position
state; strategy/execution caches reconcile against it (spec §3.5).

The engine is repository-agnostic. Production wiring binds it to the
SQLAlchemy repositories; tests bind in-memory dicts via the same
:class:`PositionStore` / :class:`BalanceStore` Protocols.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol, runtime_checkable
from uuid import UUID

from services.reconciliation.position_math import (
    apply_fill_to_balance,
    apply_fill_to_position,
)
from shared.domain import Fill

USDC = "USDC"


@runtime_checkable
class PositionStore(Protocol):
    async def get(
        self, user_id: UUID, market_id: str, token_id: str
    ) -> tuple[Decimal, Decimal, Decimal] | None:
        """Return ``(size, average_cost, realized_pnl)`` or None if absent."""

    async def upsert(
        self,
        *,
        user_id: UUID,
        market_id: str,
        token_id: str,
        size: Decimal,
        average_cost: Decimal,
        realized_pnl: Decimal,
    ) -> None: ...


@runtime_checkable
class BalanceStore(Protocol):
    async def get(self, user_id: UUID, asset: str) -> Decimal: ...

    async def upsert(self, user_id: UUID, asset: str, balance: Decimal) -> None: ...


class ReconciliationEngine:
    def __init__(self, positions: PositionStore, balances: BalanceStore) -> None:
        self._positions = positions
        self._balances = balances

    async def apply(self, fill: Fill) -> None:
        prior = await self._positions.get(fill.user_id, fill.market_id, fill.token_id)
        prior_size, prior_avg, prior_realized = prior or (Decimal(0), Decimal(0), Decimal(0))
        update = apply_fill_to_position(
            prior_size=prior_size,
            prior_avg_cost=prior_avg,
            fill_side=fill.side,
            fill_size=fill.size,
            fill_price=fill.price,
        )
        await self._positions.upsert(
            user_id=fill.user_id,
            market_id=fill.market_id,
            token_id=fill.token_id,
            size=update.new_size,
            average_cost=update.new_average_cost,
            realized_pnl=prior_realized + update.realized_pnl_delta,
        )

        prior_balance = await self._balances.get(fill.user_id, USDC)
        new_balance = apply_fill_to_balance(
            prior_balance=prior_balance,
            fill_side=fill.side,
            fill_size=fill.size,
            fill_price=fill.price,
            fee=fill.fee,
        )
        await self._balances.upsert(fill.user_id, USDC, new_balance)
