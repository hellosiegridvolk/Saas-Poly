from decimal import Decimal

import pytest

from shared.polymarket import (
    PRICE_TICK,
    SIZE_TICK,
    quantize_price,
    quantize_size,
    usdc_from_raw,
)


class TestQuantizePrice:
    def test_truncates_below_tick(self) -> None:
        assert quantize_price(Decimal("0.9199")) == Decimal("0.919")

    def test_never_rounds_up(self) -> None:
        assert quantize_price(Decimal("0.9119999999")) == Decimal("0.911")

    def test_idempotent_on_tick_aligned_value(self) -> None:
        assert quantize_price(Decimal("0.910")) == Decimal("0.910")

    def test_accepts_strings_and_ints(self) -> None:
        assert quantize_price("0.5") == Decimal("0.500")
        assert quantize_price(0) == Decimal("0.000")

    def test_output_precision_matches_tick(self) -> None:
        result = quantize_price(Decimal("0.1"))
        assert result.as_tuple().exponent == PRICE_TICK.as_tuple().exponent


class TestQuantizeSize:
    def test_truncates_below_tick(self) -> None:
        assert quantize_size(Decimal("10.1234567")) == Decimal("10.123456")

    def test_never_rounds_up_into_oversize(self) -> None:
        assert quantize_size(Decimal("10.9999999")) == Decimal("10.999999")

    def test_output_precision_matches_tick(self) -> None:
        result = quantize_size(Decimal("1"))
        assert result.as_tuple().exponent == SIZE_TICK.as_tuple().exponent


class TestUsdcFromRaw:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (1_000_000, Decimal("1")),
            (0, Decimal("0")),
            (1, Decimal("0.000001")),
            (12_345_678, Decimal("12.345678")),
        ],
    )
    def test_known_values(self, raw: int, expected: Decimal) -> None:
        assert usdc_from_raw(raw) == expected
