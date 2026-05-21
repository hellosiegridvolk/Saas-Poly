from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base

PNL_PRECISION = Numeric(30, 6)


class RiskState(Base):
    """Daily PnL, exposure, kill-switch flags per user (spec §8)."""

    __tablename__ = "risk_state"

    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    kill_switch_on: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    kill_switch_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    daily_realized_pnl: Mapped[Decimal] = mapped_column(
        PNL_PRECISION, nullable=False, default=Decimal(0)
    )
    daily_peak_pnl: Mapped[Decimal] = mapped_column(
        PNL_PRECISION, nullable=False, default=Decimal(0)
    )
    daily_drawdown: Mapped[Decimal] = mapped_column(
        PNL_PRECISION, nullable=False, default=Decimal(0)
    )
    last_reset_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
