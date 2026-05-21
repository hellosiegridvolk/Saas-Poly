from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base

SIZE_PRECISION = Numeric(30, 6)
PRICE_PRECISION = Numeric(6, 3)
FEE_PRECISION = Numeric(30, 6)


class Signal(Base):
    """Append-only log of strategy emissions (spec §8)."""

    __tablename__ = "signals"
    __table_args__ = (
        CheckConstraint("side IN ('buy', 'sell')", name="ck_signals_side"),
        CheckConstraint(
            "time_in_force IN ('GTC', 'FAK', 'IOC')", name="ck_signals_time_in_force"
        ),
        CheckConstraint("size > 0", name="ck_signals_size_positive"),
        CheckConstraint(
            "limit_price > 0 AND limit_price < 1", name="ck_signals_limit_price_bounded"
        ),
        Index("ix_signals_user_emitted_at", "user_id", "emitted_at"),
        Index("ix_signals_instance_emitted_at", "strategy_instance_id", "emitted_at"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_instance_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("strategy_instances.id", ondelete="CASCADE"),
        nullable=False,
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    size: Mapped[Decimal] = mapped_column(SIZE_PRECISION, nullable=False)
    limit_price: Mapped[Decimal] = mapped_column(PRICE_PRECISION, nullable=False)
    time_in_force: Mapped[str] = mapped_column(String(4), nullable=False, default="FAK")
    rationale: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    emitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Intent(Base):
    """Append-only post-risk log. Holds both approved and rejected outcomes (spec §8, §9)."""

    __tablename__ = "intents"
    __table_args__ = (
        CheckConstraint("decision IN ('approved', 'rejected')", name="ck_intents_decision"),
        CheckConstraint("side IN ('buy', 'sell')", name="ck_intents_side"),
        CheckConstraint(
            "time_in_force IN ('GTC', 'FAK', 'IOC')", name="ck_intents_time_in_force"
        ),
        Index("ix_intents_user_decided_at", "user_id", "decided_at"),
        Index("ix_intents_signal_id", "signal_id", unique=True),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    signal_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("signals.id", ondelete="RESTRICT"), nullable=False
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_instance_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("strategy_instances.id", ondelete="CASCADE"),
        nullable=False,
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    size: Mapped[Decimal] = mapped_column(SIZE_PRECISION, nullable=False)
    limit_price: Mapped[Decimal] = mapped_column(PRICE_PRECISION, nullable=False)
    time_in_force: Mapped[str] = mapped_column(String(4), nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    risk_decisions: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Order(Base):
    """CLOB submissions. Mutable: status, last_heartbeat_at change during lifecycle."""

    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("side IN ('buy', 'sell')", name="ck_orders_side"),
        CheckConstraint(
            "time_in_force IN ('GTC', 'FAK', 'IOC')", name="ck_orders_time_in_force"
        ),
        CheckConstraint("mode IN ('paper', 'canary', 'live')", name="ck_orders_mode"),
        CheckConstraint(
            "status IN ('pending', 'submitted', 'partially_filled', 'filled', 'canceled', 'rejected')",
            name="ck_orders_status",
        ),
        Index("ix_orders_user_status", "user_id", "status"),
        Index("ix_orders_intent_id", "intent_id"),
        Index(
            "uq_orders_clob_order_id",
            "clob_order_id",
            unique=True,
            postgresql_where="clob_order_id IS NOT NULL",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    intent_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("intents.id", ondelete="RESTRICT"), nullable=False
    )
    clob_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    size: Mapped[Decimal] = mapped_column(SIZE_PRECISION, nullable=False)
    limit_price: Mapped[Decimal] = mapped_column(PRICE_PRECISION, nullable=False)
    time_in_force: Mapped[str] = mapped_column(String(4), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Fill(Base):
    """Append-only fill ledger (spec §8). clob_fill_id is the CLOB-assigned identifier."""

    __tablename__ = "fills"
    __table_args__ = (
        CheckConstraint("side IN ('buy', 'sell')", name="ck_fills_side"),
        CheckConstraint("size > 0", name="ck_fills_size_positive"),
        CheckConstraint(
            "price > 0 AND price < 1", name="ck_fills_price_bounded"
        ),
        Index("ix_fills_user_filled_at", "user_id", "filled_at"),
        Index("ix_fills_order_id", "order_id"),
        Index("uq_fills_clob_fill_id", "clob_fill_id", unique=True),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    clob_fill_id: Mapped[str] = mapped_column(String(128), nullable=False)
    order_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("orders.id", ondelete="RESTRICT"), nullable=False
    )
    clob_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    intent_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("intents.id", ondelete="RESTRICT"), nullable=False
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    size: Mapped[Decimal] = mapped_column(SIZE_PRECISION, nullable=False)
    price: Mapped[Decimal] = mapped_column(PRICE_PRECISION, nullable=False)
    fee: Mapped[Decimal] = mapped_column(FEE_PRECISION, nullable=False)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
