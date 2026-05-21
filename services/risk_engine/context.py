"""Read-only state surface for risk gates (spec §10).

A gate must never mutate state; it only reads from a RiskContext and
returns a GateDecision. The concrete implementation lives in the per-user
risk-engine process and is backed by the repositories, the market-data
cache, and Redis. For tests, a fake context implements the same Protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class StrategyInstanceState:
    strategy_instance_id: UUID
    user_id: UUID
    strategy_id: str
    mode: Literal["paper", "canary", "live"]
    status: Literal["active", "paused", "archived"]


@dataclass(frozen=True)
class MarketSnapshot:
    market_id: str
    token_id: str
    resolved: bool
    last_tick_at: datetime
    last_book_update_at: datetime
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_depth_at_best: Decimal
    ask_depth_at_best: Decimal
    volatility_cooldown_until: datetime | None

    @property
    def mid(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal(2)

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid


@dataclass(frozen=True)
class UserState:
    user_id: UUID
    active: bool
    suspended: bool


@dataclass(frozen=True)
class RiskState:
    kill_switch_on: bool
    global_kill_switch_on: bool


@runtime_checkable
class RiskContext(Protocol):
    user_id: UUID

    async def get_user_state(self) -> UserState: ...
    async def get_strategy_instance(
        self, strategy_instance_id: UUID
    ) -> StrategyInstanceState | None: ...
    async def get_risk_state(self) -> RiskState: ...
    async def is_signal_processed(self, signal_id: UUID) -> bool: ...
    async def get_market_snapshot(
        self, market_id: str, token_id: str
    ) -> MarketSnapshot | None: ...
    async def get_position_size(self, market_id: str, token_id: str) -> Decimal: ...
    async def get_per_strategy_exposure(
        self, strategy_instance_id: UUID
    ) -> Decimal: ...
    async def get_user_aggregate_exposure(self) -> Decimal: ...
    async def get_usdc_balance(self) -> Decimal: ...
    async def now(self) -> datetime: ...
