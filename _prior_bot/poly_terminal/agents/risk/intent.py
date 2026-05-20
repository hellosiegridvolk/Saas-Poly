"""Typed BuyIntent — the input to the gate pipeline.

Defined here (rather than in `bus/models.py`) because it's primarily owned
by the Strategy Agent and consumed by Risk + Execution. Other bus payloads
remain free-form dicts; intents are first-class because every gate inspects
their fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from poly_terminal.agents.strategy.exit_config import ExitConfig
from poly_terminal.shared.enums import IntentSide, IntentSource


@dataclass(frozen=True)
class BuyIntent:
    intent_id: str
    strategy: str
    market_id: str
    token_id: str
    side: IntentSide
    size_usd: Decimal
    limit_price: Decimal
    source_wallet: Optional[str] = None      # for copy_trade
    source_size_usd: Optional[Decimal] = None
    source: IntentSource = IntentSource.MANUAL
    created_at: float = 0.0
    end_date_iso: Optional[str] = None       # market resolution time
    spread_cents: Optional[Decimal] = None   # captured at intent time
    book_depth_usd: Optional[Decimal] = None # depth on the buy side
    tick_size: Optional[Decimal] = None
    exit_config: ExitConfig = field(default_factory=ExitConfig)
