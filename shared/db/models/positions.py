from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Numeric, PrimaryKeyConstraint, String, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base

SIZE_PRECISION = Numeric(30, 6)
AVG_COST_PRECISION = Numeric(10, 6)
PNL_PRECISION = Numeric(30, 6)
BALANCE_PRECISION = Numeric(30, 6)


class Position(Base):
    """Current position per (user, market, token). Maintained by the reconciler (spec §3.5)."""

    __tablename__ = "positions"
    __table_args__ = (
        PrimaryKeyConstraint("user_id", "market_id", "token_id", name="pk_positions"),
    )

    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), nullable=False)
    size: Mapped[Decimal] = mapped_column(SIZE_PRECISION, nullable=False)
    average_cost: Mapped[Decimal] = mapped_column(AVG_COST_PRECISION, nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(PNL_PRECISION, nullable=False, default=Decimal(0))
    unrealized_pnl: Mapped[Decimal] = mapped_column(
        PNL_PRECISION, nullable=False, default=Decimal(0)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class PositionSnapshot(Base):
    """Hourly historical snapshots for analytics (spec §8)."""

    __tablename__ = "position_snapshots"
    __table_args__ = (
        Index("ix_position_snapshots_user_snapshot_at", "user_id", "snapshot_at"),
        Index(
            "ix_position_snapshots_user_market_token_snapshot_at",
            "user_id",
            "market_id",
            "token_id",
            "snapshot_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), nullable=False)
    size: Mapped[Decimal] = mapped_column(SIZE_PRECISION, nullable=False)
    average_cost: Mapped[Decimal] = mapped_column(AVG_COST_PRECISION, nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(PNL_PRECISION, nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(PNL_PRECISION, nullable=False)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Balance(Base):
    """Latest USDC + per-token balances per user (spec §8). asset = 'USDC' or token_id."""

    __tablename__ = "balances"
    __table_args__ = (
        PrimaryKeyConstraint("user_id", "asset", name="pk_balances"),
    )

    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    asset: Mapped[str] = mapped_column(String(128), nullable=False)
    balance: Mapped[Decimal] = mapped_column(BALANCE_PRECISION, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
