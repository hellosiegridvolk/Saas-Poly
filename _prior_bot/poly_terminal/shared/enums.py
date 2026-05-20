"""Stable enum types used across agents and persistence."""

from __future__ import annotations

from enum import Enum


class BotMode(str, Enum):
    """Top-level mode lock — see ADR 0001 + ADR 0004.

    Modes ordered by increasing live-execution privilege:
      READ_ONLY    : observe + record only. No intents emitted at all.
      PAPER        : intents flow through gates; fills are simulated.
      LIVE_DRY     : sign real orders but never POST. Exercises the
                     full sign path (EIP-712, SDK rounding) without
                     network exposure.
      CLOSE_ONLY   : LIVE-equivalent execution, but BUY intents are
                     rejected at the mode_lock gate. Allows SELL
                     (close) and CANCEL flows. 2026-05-05 — added for
                     the live-canary preflight: lets the operator
                     verify auth/balance/WS/positions on real Polymarket
                     state without taking any new inventory.
      LIVE         : full execution.
    """

    READ_ONLY = "READ_ONLY"
    PAPER = "PAPER"
    LIVE_DRY = "LIVE_DRY"
    CLOSE_ONLY = "CLOSE_ONLY"
    LIVE = "LIVE"


class IntentSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class IntentSource(str, Enum):
    COPY_TRADE = "COPY_TRADE"
    FLASH_CRASH = "FLASH_CRASH"
    SCALP_WINDOW = "SCALP_WINDOW"
    DUMP_HEDGE = "DUMP_HEDGE"
    ENDGAME_YIELD = "ENDGAME_YIELD"
    MANUAL = "MANUAL"


class StrategyMode(str, Enum):
    DISABLED = "DISABLED"
    PAPER = "PAPER"
    LIVE = "LIVE"


class ExitDecision(str, Enum):
    HOLD = "HOLD"
    EXIT_TP = "EXIT_TP"
    EXIT_SL = "EXIT_SL"
    EXIT_TIME = "EXIT_TIME"
    EXIT_WHALE_OUT = "EXIT_WHALE_OUT"
    # Fired by ProfitTakerAgent: closes any position whose unrealized
    # PnL crosses the absolute "10¢ per $1 of cost basis" threshold
    # (default = +10% on cost). Independent of ExitDecisionEngine —
    # no adverse_ticks_required confirmation, fires on first tick.
    EXIT_TP_ABS = "EXIT_TP_ABS"
    EXIT_SL_ABS = "EXIT_SL_ABS"  # symmetric: same agent, loss side
    EXIT_TP_TRAIL = "EXIT_TP_TRAIL"  # trailing-profit fire (locked gain)
    # 2026-05-08 PHASE 27 — trailing stop-loss. Once the position has
    # been favorable enough to "arm" (peak >= entry × (1 + sl_arm_pct),
    # default +2%), the SL switches from a fixed entry-relative threshold
    # to a peak-relative trail floor (peak × (1 - sl_trail_pct), default
    # 5% giveback from peak). Captures gains that would otherwise round-
    # trip back to entry-level SL fires. Fires from ExitDecisionEngine.
    EXIT_SL_TRAIL = "EXIT_SL_TRAIL"
