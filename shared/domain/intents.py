from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class GateDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    gate_name: str
    passed: bool
    reason: str | None
    measured_at: datetime
    duration_ms: int


class Intent(BaseModel):
    """A signal that has passed risk. Ready for execution."""

    model_config = ConfigDict(frozen=True)

    intent_id: UUID
    signal_id: UUID
    user_id: UUID
    strategy_id: str
    strategy_instance_id: UUID
    market_id: str
    token_id: str
    side: Literal["buy", "sell"]
    size: Decimal
    limit_price: Decimal
    time_in_force: Literal["GTC", "FAK", "IOC"]
    risk_decisions: list[GateDecision]
    approved_at: datetime
