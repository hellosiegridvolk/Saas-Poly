from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from shared.domain import Signal


def test_signal_roundtrip_uses_decimal() -> None:
    sig = Signal(
        signal_id=uuid4(),
        user_id=uuid4(),
        strategy_id="ninety_cent",
        strategy_instance_id=uuid4(),
        market_id="0xmarket",
        token_id="0xtoken",
        side="buy",
        size=Decimal("10.000000"),
        limit_price=Decimal("0.910"),
        rationale={"reason": "smoke"},
        emitted_at=datetime.now(tz=UTC),
    )
    assert isinstance(sig.size, Decimal)
    assert isinstance(sig.limit_price, Decimal)
    assert sig.time_in_force == "FAK"
