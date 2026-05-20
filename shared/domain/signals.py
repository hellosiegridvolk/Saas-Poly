from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class Signal(BaseModel):
    """Emitted by a strategy. Pure intent, never an order."""

    model_config = ConfigDict(frozen=True)

    signal_id: UUID
    user_id: UUID
    strategy_id: str
    strategy_instance_id: UUID
    market_id: str
    token_id: str
    side: Literal["buy", "sell"]
    size: Decimal
    limit_price: Decimal
    time_in_force: Literal["GTC", "FAK", "IOC"] = "FAK"
    rationale: dict[str, object]
    emitted_at: datetime
