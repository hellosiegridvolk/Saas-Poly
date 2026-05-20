from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class Fill(BaseModel):
    model_config = ConfigDict(frozen=True)

    fill_id: str
    order_id: str
    intent_id: UUID
    user_id: UUID
    market_id: str
    token_id: str
    side: Literal["buy", "sell"]
    size: Decimal
    price: Decimal
    fee: Decimal
    filled_at: datetime
