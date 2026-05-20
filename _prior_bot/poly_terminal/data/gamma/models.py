"""Typed Gamma API response models.

Returning typed objects (not raw dicts) keeps the rest of the codebase
ignorant of API shape — schema changes get caught at the boundary.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field


class GammaToken(BaseModel):
    """A single CLOB token (YES or NO leg of an event)."""

    token_id: str
    outcome: str  # "Yes" / "No" / "Up" / "Down"
    price: Decimal | None = None


class GammaMarket(BaseModel):
    """Tradable market with a CLOB book."""

    condition_id: str
    slug: str
    question: str
    end_date_iso: str | None = None
    active: bool = True
    closed: bool = False
    enable_orderbook: bool = True
    liquidity_usd: Decimal = Decimal("0")
    volume_24hr: Decimal = Decimal("0")
    tokens: list[GammaToken] = Field(default_factory=list)

    def yes_token(self) -> GammaToken | None:
        for t in self.tokens:
            if t.outcome.lower() in ("yes", "up"):
                return t
        return None

    def no_token(self) -> GammaToken | None:
        for t in self.tokens:
            if t.outcome.lower() in ("no", "down"):
                return t
        return None

    def is_tradable(self) -> bool:
        return (
            self.active
            and not self.closed
            and self.enable_orderbook
            and len(self.tokens) >= 2
        )


class GammaEvent(BaseModel):
    """Top-level event (may contain multiple markets)."""

    event_id: str
    slug: str
    title: str | None = None
    end_date_iso: str | None = None
    active: bool = True
    closed: bool = False
    markets: list[GammaMarket] = Field(default_factory=list)

    def first_tradable_market(self) -> GammaMarket | None:
        for m in self.markets:
            if m.is_tradable():
                return m
        return None


class GammaTag(BaseModel):
    tag_id: int
    label: str
    slug: str | None = None


def parse_event(payload: dict[str, Any]) -> GammaEvent:
    """Parse a raw `/events?slug=...` response item into a typed GammaEvent."""
    import json

    markets: list[GammaMarket] = []
    for m in payload.get("markets", []):
        token_ids: list[str] = []
        outcomes: list[str] = []
        raw_token_ids = m.get("clobTokenIds")
        raw_outcomes = m.get("outcomes")
        if isinstance(raw_token_ids, str) and raw_token_ids:
            try:
                token_ids = list(json.loads(raw_token_ids))
            except (ValueError, TypeError):
                token_ids = []
        elif isinstance(raw_token_ids, list):
            token_ids = list(raw_token_ids)
        if isinstance(raw_outcomes, str) and raw_outcomes:
            try:
                outcomes = list(json.loads(raw_outcomes))
            except (ValueError, TypeError):
                outcomes = []
        elif isinstance(raw_outcomes, list):
            outcomes = list(raw_outcomes)
        tokens = [
            GammaToken(token_id=str(tid), outcome=str(out))
            for tid, out in zip(token_ids, outcomes, strict=False)
        ]
        markets.append(
            GammaMarket(
                condition_id=str(m.get("conditionId", "")),
                slug=str(m.get("slug", "")),
                question=str(m.get("question", "")),
                end_date_iso=m.get("endDate"),
                active=bool(m.get("active", True)),
                closed=bool(m.get("closed", False)),
                enable_orderbook=bool(m.get("enableOrderBook", True)),
                liquidity_usd=Decimal(str(m.get("liquidity", "0"))),
                volume_24hr=Decimal(str(m.get("volume24hr", "0"))),
                tokens=tokens,
            )
        )
    return GammaEvent(
        event_id=str(payload.get("id", "")),
        slug=str(payload.get("slug", "")),
        title=payload.get("title"),
        end_date_iso=payload.get("endDate"),
        active=bool(payload.get("active", True)),
        closed=bool(payload.get("closed", False)),
        markets=markets,
    )
