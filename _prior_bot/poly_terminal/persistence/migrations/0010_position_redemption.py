"""Persist redemption status on positions.

Polymarket V2 binary outcomes settle on-chain via
`ConditionalTokens.redeemPositions(...)`. Until our funder is
authorized via Polymarket's relayer (or we wire web3 + Magic-Link
proxy execution), redemption is operator-driven. We track per-position
redemption state so:

- WORTHLESS positions (we held the losing side at resolution) are
  auto-marked `redeemed_ts=now, redeem_tx_hash='WORTHLESS_NO_TX'` —
  no tx needed, just clears the queue.
- REDEEMABLE positions (we held the winning side) wait until the
  operator runs the manual redemption on the Polymarket UI; the
  status surface keeps a running "needs-redemption" total so the
  operator nudge is visible.
- After A/B redemption flow lands, the agent populates
  `redeem_tx_hash` with the real on-chain hash for audit.

Both columns are nullable so legacy rows (closed before this
migration) are treated as "unknown" by the redeemer agent and
get a one-shot classification on next sweep.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE positions ADD COLUMN redeemed_ts INTEGER",
    "ALTER TABLE positions ADD COLUMN redeem_tx_hash TEXT",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
