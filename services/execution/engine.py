"""Paper-mode execution engine (spec §11, §15 Phase 1 step 6).

Consumes approved Intents, simulates fills against the latest book
snapshot via :mod:`services.execution.paper_filler`, and returns the
resulting Fill + Order pair. Idempotent on ``signal_id`` (spec §3.4):
two intents with the same signal_id never produce two orders.

Real CLOB submission, FAK/IOC handling, cancel/replace, and live
heartbeat plumbing land in the live-mode PR.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from services.execution.paper_filler import BookSnapshot, simulate_fill
from shared.domain import Fill, Intent


@dataclass(frozen=True)
class PaperOrder:
    order_id: UUID
    intent_id: UUID
    user_id: UUID
    market_id: str
    token_id: str
    side: str
    size: Decimal
    limit_price: Decimal
    time_in_force: str
    status: str
    submitted_at: datetime


@dataclass(frozen=True)
class ExecutionResult:
    order: PaperOrder
    fill: Fill | None
    """``None`` if the intent was duplicated (idempotency hit) or the book
    couldn't fill any size."""


BookProvider = Callable[[str, str], Awaitable[BookSnapshot | None]]


class PaperExecutionEngine:
    """Single-user paper-mode executor."""

    def __init__(self, *, book_provider: BookProvider) -> None:
        self._book_provider = book_provider
        self._seen_signal_ids: set[UUID] = set()

    async def submit(self, intent: Intent) -> ExecutionResult | None:
        if intent.signal_id in self._seen_signal_ids:
            return None
        self._seen_signal_ids.add(intent.signal_id)

        book = await self._book_provider(intent.market_id, intent.token_id)
        order = PaperOrder(
            order_id=uuid4(),
            intent_id=intent.intent_id,
            user_id=intent.user_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=intent.side,
            size=intent.size,
            limit_price=intent.limit_price,
            time_in_force=intent.time_in_force,
            status="submitted",
            submitted_at=datetime.now(tz=UTC),
        )

        fill: Fill | None = None
        if book is not None:
            result = simulate_fill(
                side=intent.side,
                size=intent.size,
                limit_price=intent.limit_price,
                book=book,
            )
            if result is not None:
                fill = Fill(
                    fill_id=str(uuid4()),
                    order_id=str(order.order_id),
                    intent_id=intent.intent_id,
                    user_id=intent.user_id,
                    market_id=intent.market_id,
                    token_id=intent.token_id,
                    side=intent.side,
                    size=result.filled_size,
                    price=result.fill_price,
                    fee=result.fee,
                    filled_at=datetime.now(tz=UTC),
                )
        return ExecutionResult(order=order, fill=fill)
