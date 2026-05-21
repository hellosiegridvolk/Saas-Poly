"""Shared fixtures for risk engine tests: a fake RiskContext with
overridable per-call state, and a Signal builder with sane defaults.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

import pytest

from services.risk_engine.context import (
    MarketSnapshot,
    RiskState,
    StrategyInstanceState,
    UserState,
)
from shared.domain import Signal


@dataclass
class FakeRiskContext:
    user_id: UUID = field(default_factory=uuid4)
    now_dt: datetime = field(default_factory=lambda: datetime(2026, 5, 21, tzinfo=UTC))
    user: UserState | None = None
    strategy_instance: StrategyInstanceState | None = None
    risk: RiskState = field(
        default_factory=lambda: RiskState(kill_switch_on=False, global_kill_switch_on=False)
    )
    processed_signal_ids: set[UUID] = field(default_factory=set)
    market: MarketSnapshot | None = None
    position_size: Decimal = Decimal(0)
    per_strategy_exposure: Decimal = Decimal(0)
    aggregate_exposure: Decimal = Decimal(0)
    usdc_balance: Decimal = Decimal("1000")

    def __post_init__(self) -> None:
        if self.user is None:
            self.user = UserState(user_id=self.user_id, active=True, suspended=False)

    async def get_user_state(self) -> UserState:
        assert self.user is not None
        return self.user

    async def get_strategy_instance(
        self, strategy_instance_id: UUID
    ) -> StrategyInstanceState | None:
        return self.strategy_instance

    async def get_risk_state(self) -> RiskState:
        return self.risk

    async def is_signal_processed(self, signal_id: UUID) -> bool:
        return signal_id in self.processed_signal_ids

    async def get_market_snapshot(
        self, market_id: str, token_id: str
    ) -> MarketSnapshot | None:
        return self.market

    async def get_position_size(self, market_id: str, token_id: str) -> Decimal:
        return self.position_size

    async def get_per_strategy_exposure(self, strategy_instance_id: UUID) -> Decimal:
        return self.per_strategy_exposure

    async def get_user_aggregate_exposure(self) -> Decimal:
        return self.aggregate_exposure

    async def get_usdc_balance(self) -> Decimal:
        return self.usdc_balance

    async def now(self) -> datetime:
        return self.now_dt


def make_signal(
    *,
    user_id: UUID | None = None,
    strategy_instance_id: UUID | None = None,
    market_id: str = "0xmarket",
    token_id: str = "0xtoken",
    side: Literal["buy", "sell"] = "buy",
    size: Decimal = Decimal("10"),
    limit_price: Decimal = Decimal("0.500"),
    time_in_force: Literal["GTC", "FAK", "IOC"] = "FAK",
) -> Signal:
    return Signal(
        signal_id=uuid4(),
        user_id=user_id or uuid4(),
        strategy_id="ninety_cent",
        strategy_instance_id=strategy_instance_id or uuid4(),
        market_id=market_id,
        token_id=token_id,
        side=side,
        size=size,
        limit_price=limit_price,
        time_in_force=time_in_force,
        rationale={"test": True},
        emitted_at=datetime.now(tz=UTC),
    )


def make_market(
    *,
    now: datetime,
    resolved: bool = False,
    best_bid: Decimal = Decimal("0.498"),
    best_ask: Decimal = Decimal("0.502"),
    bid_depth: Decimal = Decimal("1000"),
    ask_depth: Decimal = Decimal("1000"),
    tick_age_s: float = 1.0,
    book_age_s: float = 1.0,
    cooldown_until: datetime | None = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="0xmarket",
        token_id="0xtoken",
        resolved=resolved,
        last_tick_at=now - timedelta(seconds=tick_age_s),
        last_book_update_at=now - timedelta(seconds=book_age_s),
        best_bid=best_bid,
        best_ask=best_ask,
        bid_depth_at_best=bid_depth,
        ask_depth_at_best=ask_depth,
        volatility_cooldown_until=cooldown_until,
    )


@pytest.fixture
def fake_ctx() -> FakeRiskContext:
    return FakeRiskContext()
