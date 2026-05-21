from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base

PAYOUT_PRECISION = Numeric(30, 6)


class Redemption(Base):
    """Claimed winnings ledger (spec §8)."""

    __tablename__ = "redemptions"
    __table_args__ = (
        Index("ix_redemptions_user_redeemed_at", "user_id", "redeemed_at"),
        Index("uq_redemptions_tx_hash", "tx_hash", unique=True),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    condition_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payout_usdc: Mapped[Decimal] = mapped_column(PAYOUT_PRECISION, nullable=False)
    tx_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    redeemed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
