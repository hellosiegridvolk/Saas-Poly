"""Strategy plugin contract (spec §12.1).

Strategies emit Signals via return values. They never reference the
execution engine, the SDK, or a wallet (spec §3.1). The StrategyContext
provides read-only state plus a logger and metrics recorder; it
deliberately does NOT expose an execution client.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from shared.domain import Fill, GateDecision, Position, Signal


class MarketTick(BaseModel):
    """A single trade or top-of-book update consumed by strategies."""

    model_config = ConfigDict(frozen=True)

    market_id: str
    token_id: str
    price: Decimal
    size: Decimal
    observed_at: datetime


class OrderBookLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal
    size: Decimal


class OrderBook(BaseModel):
    model_config = ConfigDict(frozen=True)

    market_id: str
    token_id: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    observed_at: datetime


class StrategyLogger(Protocol):
    def info(self, event: str, **fields: object) -> None: ...
    def warning(self, event: str, **fields: object) -> None: ...
    def error(self, event: str, **fields: object) -> None: ...


class StrategyMetrics(Protocol):
    def increment(self, name: str, value: float = 1.0, **tags: str) -> None: ...
    def observe(self, name: str, value: float, **tags: str) -> None: ...


@runtime_checkable
class StrategyContext(Protocol):
    """Read-only state surface provided to a Strategy at runtime.

    Implementations live in services/strategy_worker. This protocol is the
    only surface a strategy is allowed to touch (spec §3.1, §12.1).
    """

    user_id: UUID
    strategy_instance_id: UUID
    config: BaseModel
    logger: StrategyLogger
    metrics: StrategyMetrics

    async def get_position(self, market_id: str, token_id: str) -> Position | None: ...
    async def get_usdc_balance(self) -> Decimal: ...
    async def get_market_metadata(self, market_id: str) -> Mapping[str, Any]: ...


@runtime_checkable
class Strategy(Protocol):
    """The plugin contract every built-in and third-party strategy implements."""

    strategy_id: ClassVar[str]
    config_model: ClassVar[type[BaseModel]]

    async def on_start(self, ctx: StrategyContext) -> None: ...
    async def on_market_tick(self, tick: MarketTick) -> list[Signal]: ...
    async def on_book_update(self, book: OrderBook) -> list[Signal]: ...
    async def on_fill(self, fill: Fill) -> list[Signal]: ...
    async def on_intent_rejected(self, decision: GateDecision) -> None: ...
    async def on_stop(self) -> None: ...
