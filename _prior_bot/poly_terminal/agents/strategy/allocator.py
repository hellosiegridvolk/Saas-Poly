"""RiskAllocator — the single path from a `StrategySignal` to capital.

Phase 32 P3 (2026-05-09) — companion to the playbook in
`docs/strategy polymarket.md` §13.

Gate stack (short-circuits at the first rejection):
  1. mode gate           — LIVE-only allow-list
  2. strategy enabled    — covered by the allow-list
  3. capital cap         — `signal.max_loss_usd <= live_position_cap_usd`
  4. position limit      — open count < `open_position_limit`
  5. exposure limit      — sum(open + this) <= `max_total_exposure_usd`
  6. one-strategy gate   — open positions all share signal.strategy_name
  7. daily loss cap      — `realized_today_usd_loss + max_loss <= cap`
  8. quarantine lock     — token_id not in `quarantined_tokens`
  9. wallet probation    — copy-* strategies need >= N PAPER fills

LIVE-only gates:
  * the allow-list, the probation floor — PAPER iterates without
    forcing the operator to keep a sandbox cohort warm.

Inputs are passive snapshots (`LedgerSnapshot`, `OpenPosition`); the
allocator never reaches into a repo. The caller (a strategy router, or
a top-level dispatcher) is responsible for materializing the snapshot
from the live state at decision time.
"""
from __future__ import annotations

from dataclasses import dataclass

from poly_terminal.agents.strategy.framework import (
    RejectReason,
    StrategyDecision,
    StrategySignal,
)
from poly_terminal.shared.enums import BotMode


# Strategies whose signals carry a wallet attribution that should
# walk through the probation floor before LIVE capital is approved.
_COPY_FAMILY_STRATEGIES = frozenset({"copy_trade", "copy_scalp"})


@dataclass(frozen=True)
class OpenPosition:
    """Subset of a position row the allocator needs.

    Mirroring the full PositionRow here would couple the allocator to
    the persistence layer; passing only what's needed keeps the
    allocator pure and trivially mockable.
    """
    position_id: int
    strategy_name: str
    token_id: str
    cost_basis_usd: float
    source_wallet: str | None = None


@dataclass(frozen=True)
class LedgerSnapshot:
    """Point-in-time bookkeeping the allocator inspects.

    `realized_today_usd` is a *signed* number — negative for losses,
    positive for gains. The daily-loss-cap gate measures additional
    LOSS capacity; gains are ignored (cap doesn't grow with profit).
    """
    open_positions: tuple[OpenPosition, ...] = ()
    realized_today_usd: float = 0.0
    quarantined_tokens: frozenset[str] = frozenset()


@dataclass(frozen=True)
class AllocatorConfig:
    bankroll_usd: float
    live_position_cap_usd: float
    open_position_limit: int
    max_total_exposure_usd: float
    daily_loss_cap_usd: float
    one_strategy_at_a_time: bool
    live_allowed: frozenset[str]
    wallet_probation_min_paper_fills: int = 5


class RiskAllocator:
    """Pure decision function — no I/O. Tests pin every gate."""

    def __init__(self, cfg: AllocatorConfig) -> None:
        self._cfg = cfg

    def approve(
        self,
        signal: StrategySignal,
        *,
        mode: BotMode,
        ledger: LedgerSnapshot,
    ) -> StrategyDecision:
        cfg = self._cfg

        # 1. LIVE-only allow-list.
        if mode == BotMode.LIVE and signal.strategy_name not in cfg.live_allowed:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.STRATEGY_DISABLED,
                detail=(
                    f"strategy {signal.strategy_name!r} not in LIVE "
                    f"allow-list ({sorted(cfg.live_allowed)})"
                ),
            )

        # 2. Capital cap (per-position).
        if signal.max_loss_usd > cfg.live_position_cap_usd + 1e-9:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.CAPITAL_CAP,
                detail=(
                    f"signal.max_loss_usd ${signal.max_loss_usd:.2f} > "
                    f"cap ${cfg.live_position_cap_usd:.2f}"
                ),
            )

        # 3. Position limit.
        if len(ledger.open_positions) >= cfg.open_position_limit:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.POSITION_LIMIT,
                detail=(
                    f"{len(ledger.open_positions)} open >= limit "
                    f"{cfg.open_position_limit}"
                ),
            )

        # 3b. Per-token dedup — only one open position per token_id at a time.
        # Prevents rapid-fire wallet signals on the same token from opening
        # multiple concurrent positions when MAX_OPEN_POSITIONS > 1.
        existing_token = next(
            (p for p in ledger.open_positions if p.token_id == signal.token_id),
            None,
        )
        if existing_token is not None:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.TOKEN_ALREADY_OPEN,
                detail=(
                    f"position {existing_token.position_id} already open "
                    f"for token {signal.token_id[:16]}…"
                ),
            )

        # 3c. Per-source-wallet dedup — max one open position per followed wallet.
        # Prevents a single prolific whale from consuming all position slots.
        _signal_wallet = getattr(signal, "source_wallet", None)
        if _signal_wallet:
            existing_wallet = next(
                (
                    p for p in ledger.open_positions
                    if p.source_wallet and p.source_wallet.lower() == _signal_wallet.lower()
                ),
                None,
            )
            if existing_wallet is not None:
                return StrategyDecision(
                    approved=False, signal=signal,
                    reason=RejectReason.TOKEN_ALREADY_OPEN,
                    detail=(
                        f"position {existing_wallet.position_id} already open "
                        f"for wallet {_signal_wallet[:12]}…"
                    ),
                )

        # 4. Total exposure.
        open_exposure = sum(p.cost_basis_usd for p in ledger.open_positions)
        if open_exposure + signal.max_loss_usd > cfg.max_total_exposure_usd + 1e-9:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.EXPOSURE_LIMIT,
                detail=(
                    f"exposure ${open_exposure:.2f} + signal "
                    f"${signal.max_loss_usd:.2f} > cap "
                    f"${cfg.max_total_exposure_usd:.2f}"
                ),
            )

        # 5. One-strategy-at-a-time (canary phase).
        if cfg.one_strategy_at_a_time:
            other = next(
                (
                    p for p in ledger.open_positions
                    if p.strategy_name != signal.strategy_name
                ),
                None,
            )
            if other is not None:
                return StrategyDecision(
                    approved=False, signal=signal,
                    reason=RejectReason.DUPLICATE_STRATEGY_OPEN,
                    detail=(
                        f"open position {other.position_id} is "
                        f"{other.strategy_name!r}, signal is "
                        f"{signal.strategy_name!r}"
                    ),
                )

        # 6. Daily loss cap (cap is on additional loss capacity).
        already_lost = max(0.0, -float(ledger.realized_today_usd))
        if already_lost + signal.max_loss_usd > cfg.daily_loss_cap_usd + 1e-9:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.DAILY_LOSS_CAP,
                detail=(
                    f"already lost ${already_lost:.2f} + signal "
                    f"${signal.max_loss_usd:.2f} > cap "
                    f"${cfg.daily_loss_cap_usd:.2f}"
                ),
            )

        # 7. Quarantine lock.
        if signal.token_id in ledger.quarantined_tokens:
            return StrategyDecision(
                approved=False, signal=signal,
                reason=RejectReason.QUARANTINED_TOKEN,
                detail=f"token_id {signal.token_id} is quarantined",
            )

        # 8. Wallet probation (LIVE-only, copy-family-only).
        if (
            mode == BotMode.LIVE
            and signal.strategy_name in _COPY_FAMILY_STRATEGIES
        ):
            paper_fills = int(signal.extra.get("wallet_paper_fills_count", 0))
            if paper_fills < cfg.wallet_probation_min_paper_fills:
                wallet = signal.extra.get("wallet_address", "<unknown>")
                return StrategyDecision(
                    approved=False, signal=signal,
                    reason=RejectReason.PROBATION_FLOOR,
                    detail=(
                        f"wallet {wallet} has {paper_fills} PAPER "
                        f"fills < probation floor "
                        f"{cfg.wallet_probation_min_paper_fills}"
                    ),
                )

        return StrategyDecision(
            approved=True, signal=signal, reason=None, detail="approved",
        )


__all__ = [
    "AllocatorConfig",
    "LedgerSnapshot",
    "OpenPosition",
    "RiskAllocator",
]
