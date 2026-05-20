from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class Position(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_id: UUID
    market_id: str
    token_id: str
    size: Decimal
    average_cost: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    updated_at: datetime
