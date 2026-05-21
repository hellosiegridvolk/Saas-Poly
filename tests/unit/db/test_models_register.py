"""Smoke test: importing shared.db.models registers every v1 table on Base.metadata.

Catches the case where a new table is added but forgotten in
shared.db.models.__init__, which would silently leave it out of
``alembic --autogenerate`` and migrations.
"""

from shared.db import models  # noqa: F401  (register)
from shared.db.base import Base

EXPECTED_TABLES = {
    "users",
    "user_wallets",
    "user_secrets",
    "subscriptions",
    "strategy_instances",
    "signals",
    "intents",
    "orders",
    "fills",
    "positions",
    "position_snapshots",
    "balances",
    "risk_state",
    "audit_log",
    "notifications",
    "redemptions",
}


def test_all_v1_tables_registered() -> None:
    registered = set(Base.metadata.tables.keys())
    missing = EXPECTED_TABLES - registered
    assert not missing, f"tables not registered on Base.metadata: {sorted(missing)}"
