"""Event-name constants used across the bus.

Using constants instead of bare strings prevents typo-driven silent
failures. Adding a new event = adding a constant here. Renaming an
event = greppable rename across the codebase.

Grouped by producer.
"""

from __future__ import annotations

from typing import Final

# ── Discovery / Context Agent ─────────────────────────────────────────
EVT_WATCHLIST_UPDATED: Final = "watchlist.updated"
EVT_CONTEXT_OK: Final = "context.ok"
EVT_CONTEXT_BLOCK: Final = "context.block"

# ── Market WebSocket / data layer ─────────────────────────────────────
EVT_MARKET_TICK: Final = "market.tick"
EVT_MARKET_TICK_SIZE: Final = "market.tick_size"
EVT_BOOK_SNAPSHOT: Final = "book.snapshot"
# 2026-05-05 (deep-research-23 item #4): best_bid_ask events emitted
# when subscribing with custom_feature_enabled=True. Carries both
# best_bid and best_ask in one payload — strictly richer than the
# price_change / last_trade_price fallback. ExitAgent uses best_bid
# directly because that's the realistic SELL-side exit price.
EVT_BEST_BID_ASK: Final = "book.best_bid_ask"

# ── Orderbook Intelligence Agent ──────────────────────────────────────
EVT_BOOK_IMBALANCE: Final = "book.imbalance"
EVT_LIQUIDITY_GAP: Final = "book.liquidity_gap"
EVT_SPOOF_WALL_DETECTED: Final = "book.spoof_wall"

# ── Polygon on-chain / CLOB price-impact signals ─────────────────────
# Option B (price_surge_detector): emitted when best_ask drops >= threshold
# from its rolling baseline on a pre-watched token.  Payload keys match
# EVT_WALLET_FILL so copy_scalp_active can consume it transparently.
EVT_PRICE_SURGE: Final = "market.price_surge"

# ── Wallet Intelligence Agent ─────────────────────────────────────────
EVT_WALLET_FILL: Final = "wallet.fill"
EVT_WALLET_REDEEM: Final = "wallet.redeem"
EVT_WALLET_SCORED: Final = "wallet.scored"
EVT_WALLET_RANK_CHANGED: Final = "wallet.rank_changed"

# ── Strategy → Risk → Execution chain ─────────────────────────────────
EVT_BUY_INTENT: Final = "intent.buy"
EVT_SELL_INTENT: Final = "intent.sell"
EVT_INTENT_APPROVED: Final = "intent.approved"
EVT_INTENT_REJECTED: Final = "intent.rejected"

# ── Order lifecycle ───────────────────────────────────────────────────
EVT_ORDER_SUBMITTED: Final = "order.submitted"
EVT_ORDER_FILLED: Final = "order.filled"
EVT_ORDER_REJECTED: Final = "order.rejected"
EVT_ORDER_CANCELLED: Final = "order.cancelled"

# ── Position lifecycle ────────────────────────────────────────────────
EVT_POSITION_OPENED: Final = "position.opened"
EVT_POSITION_CLOSED: Final = "position.closed"

# ── System health ─────────────────────────────────────────────────────
EVT_DB_DEGRADED: Final = "system.db_degraded"
EVT_LATENCY_BUDGET_BREACH: Final = "system.latency_budget_breach"
EVT_AGENT_HEARTBEAT: Final = "system.agent_heartbeat"
