"""Per-position runtime state owned by the Exit Agent.

Tracks adverse-tick counter and last-seen price; provides arithmetic
helpers (unrealized PnL, percent move) that the decision engine consumes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class PositionState:
    position_id: int
    token_id: str
    entry_price: Decimal
    shares: Decimal
    cost_basis_usd: Decimal
    entry_ts: float
    adverse_tick_count: int = 0
    last_price: Decimal = field(default_factory=lambda: Decimal("0"))
    # 2026-05-05 (deep-research-23 item #5): bar-end timestamp parsed
    # from end_date_iso at position open. None when the market doesn't
    # have a fixed resolution window (e.g. always-open markets) or when
    # we couldn't parse the ISO string. Decision engine uses this to
    # cap the warmup window proportional to time-remaining-at-entry.
    bar_end_ts: float | None = None
    # 2026-05-08 PHASE 27 — peak price seen post-entry. Used by the
    # adaptive trailing-SL gate: once peak rises above
    # `entry × (1 + sl_arm_pct)`, the SL converts from entry-relative
    # fixed to peak-relative trailing. Defaults to 0 (= "no peak yet")
    # so the engine treats the first observation as the initial peak.
    # Updated by `observe_tick`.
    peak_price: Decimal = field(default_factory=lambda: Decimal("0"))

    def unrealized_usd(self, current_price: Decimal) -> Decimal:
        return (current_price - self.entry_price) * self.shares

    def pct_move(self, current_price: Decimal) -> Decimal:
        if self.cost_basis_usd == 0:
            return Decimal("0")
        return self.unrealized_usd(current_price) / self.cost_basis_usd

    def observe_tick(self, current_price: Decimal) -> None:
        """Update adverse-tick counter.

        First tick is considered adverse iff price < entry_price.
        Subsequent ticks: adverse iff price < previous tick.
        Any non-adverse tick resets the counter.

        2026-05-06 — `last_price == Decimal("0")` is technically a
        stale-init sentinel here AND a legitimate price (a fully-
        resolved-down outcome). In practice this is fine because
        ExitDecisionEngine.evaluate_with_reason filters all price<=0
        ticks BEFORE calling observe_tick — see BLOCK_INVALID_PRICE.
        Consumers that bypass the engine (none in production today)
        would re-introduce the ambiguity, so keep this method
        defensive: changing the sentinel here cascades through 12+
        consumers (`pos.last_price > 0` etc.). Filter at the engine
        boundary instead.
        """
        if self.last_price == Decimal("0"):
            # First observation — compare to entry price.
            adverse = current_price < self.entry_price
        else:
            adverse = current_price < self.last_price
        if adverse:
            self.adverse_tick_count += 1
        else:
            self.adverse_tick_count = 0
        self.last_price = current_price
        # 2026-05-08 PHASE 27 — track peak for trailing-SL.
        # Initial peak = max(entry, first_observation) so a position
        # that opens already-profitable starts with the right anchor.
        if self.peak_price == Decimal("0"):
            self.peak_price = max(self.entry_price, current_price)
        elif current_price > self.peak_price:
            self.peak_price = current_price
