"""full v1 schema: wallets, secrets, billing, strategy instances, trading ledger, positions, risk, audit, notifications, redemptions

Adds the 15 tables sketched in spec §8, plus an append-only trigger on
signals, intents, fills and audit_log (spec §8 footer).

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-21

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APPEND_ONLY_TABLES = ("signals", "intents", "fills", "audit_log")


def upgrade() -> None:
    op.create_table(
        "user_wallets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("address", sa.String(length=42), nullable=False),
        sa.Column("proxy_wallet_address", sa.String(length=42), nullable=True),
        sa.Column("chain_id", sa.Integer(), nullable=False, server_default="137"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("linked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("unlinked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_user_wallets_user_id", "user_wallets", ["user_id"])
    op.create_index(
        "uq_user_wallets_user_address", "user_wallets", ["user_id", "address"], unique=True
    )

    op.create_table(
        "user_secrets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("key_version", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column("encrypted_blob", sa.LargeBinary(), nullable=False),
        sa.Column("nonce", sa.LargeBinary(), nullable=False),
        sa.Column("aad", sa.LargeBinary(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "uq_user_secrets_user_scope", "user_secrets", ["user_id", "scope"], unique=True
    )

    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("stripe_customer_id", sa.String(length=64), nullable=False, unique=True),
        sa.Column("stripe_subscription_id", sa.String(length=64), nullable=True),
        sa.Column("plan", sa.String(length=32), nullable=False, server_default="free"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    op.create_table(
        "strategy_instances",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False, server_default="paper"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("config", postgresql.JSONB(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "mode IN ('paper', 'canary', 'live')", name="ck_strategy_instances_mode"
        ),
        sa.CheckConstraint(
            "status IN ('active', 'paused', 'archived')", name="ck_strategy_instances_status"
        ),
    )
    op.create_index(
        "ix_strategy_instances_user_status", "strategy_instances", ["user_id", "status"]
    )
    op.create_index(
        "ix_strategy_instances_user_strategy",
        "strategy_instances",
        ["user_id", "strategy_id"],
    )

    op.create_table(
        "signals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column(
            "strategy_instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("side", sa.String(length=4), nullable=False),
        sa.Column("size", sa.Numeric(30, 6), nullable=False),
        sa.Column("limit_price", sa.Numeric(6, 3), nullable=False),
        sa.Column("time_in_force", sa.String(length=4), nullable=False, server_default="FAK"),
        sa.Column("rationale", postgresql.JSONB(), nullable=False),
        sa.Column("emitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_signals_side"),
        sa.CheckConstraint(
            "time_in_force IN ('GTC', 'FAK', 'IOC')", name="ck_signals_time_in_force"
        ),
        sa.CheckConstraint("size > 0", name="ck_signals_size_positive"),
        sa.CheckConstraint(
            "limit_price > 0 AND limit_price < 1", name="ck_signals_limit_price_bounded"
        ),
    )
    op.create_index("ix_signals_user_emitted_at", "signals", ["user_id", "emitted_at"])
    op.create_index(
        "ix_signals_instance_emitted_at", "signals", ["strategy_instance_id", "emitted_at"]
    )

    op.create_table(
        "intents",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "signal_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("signals.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column(
            "strategy_instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("side", sa.String(length=4), nullable=False),
        sa.Column("size", sa.Numeric(30, 6), nullable=False),
        sa.Column("limit_price", sa.Numeric(6, 3), nullable=False),
        sa.Column("time_in_force", sa.String(length=4), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("risk_decisions", postgresql.JSONB(), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("decision IN ('approved', 'rejected')", name="ck_intents_decision"),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_intents_side"),
        sa.CheckConstraint(
            "time_in_force IN ('GTC', 'FAK', 'IOC')", name="ck_intents_time_in_force"
        ),
    )
    op.create_index("ix_intents_user_decided_at", "intents", ["user_id", "decided_at"])
    op.create_index("ix_intents_signal_id", "intents", ["signal_id"], unique=True)

    op.create_table(
        "orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "intent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("intents.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("clob_order_id", sa.String(length=128), nullable=True),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("side", sa.String(length=4), nullable=False),
        sa.Column("size", sa.Numeric(30, 6), nullable=False),
        sa.Column("limit_price", sa.Numeric(6, 3), nullable=False),
        sa.Column("time_in_force", sa.String(length=4), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_orders_side"),
        sa.CheckConstraint(
            "time_in_force IN ('GTC', 'FAK', 'IOC')", name="ck_orders_time_in_force"
        ),
        sa.CheckConstraint("mode IN ('paper', 'canary', 'live')", name="ck_orders_mode"),
        sa.CheckConstraint(
            "status IN ('pending', 'submitted', 'partially_filled', 'filled', 'canceled', 'rejected')",
            name="ck_orders_status",
        ),
    )
    op.create_index("ix_orders_user_status", "orders", ["user_id", "status"])
    op.create_index("ix_orders_intent_id", "orders", ["intent_id"])
    op.create_index(
        "uq_orders_clob_order_id",
        "orders",
        ["clob_order_id"],
        unique=True,
        postgresql_where=sa.text("clob_order_id IS NOT NULL"),
    )

    op.create_table(
        "fills",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("clob_fill_id", sa.String(length=128), nullable=False),
        sa.Column(
            "order_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orders.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("clob_order_id", sa.String(length=128), nullable=True),
        sa.Column(
            "intent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("intents.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("side", sa.String(length=4), nullable=False),
        sa.Column("size", sa.Numeric(30, 6), nullable=False),
        sa.Column("price", sa.Numeric(6, 3), nullable=False),
        sa.Column("fee", sa.Numeric(30, 6), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_fills_side"),
        sa.CheckConstraint("size > 0", name="ck_fills_size_positive"),
        sa.CheckConstraint("price > 0 AND price < 1", name="ck_fills_price_bounded"),
    )
    op.create_index("ix_fills_user_filled_at", "fills", ["user_id", "filled_at"])
    op.create_index("ix_fills_order_id", "fills", ["order_id"])
    op.create_index("uq_fills_clob_fill_id", "fills", ["clob_fill_id"], unique=True)

    op.create_table(
        "positions",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("size", sa.Numeric(30, 6), nullable=False),
        sa.Column("average_cost", sa.Numeric(10, 6), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column("unrealized_pnl", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("user_id", "market_id", "token_id", name="pk_positions"),
    )

    op.create_table(
        "position_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("size", sa.Numeric(30, 6), nullable=False),
        sa.Column("average_cost", sa.Numeric(10, 6), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(30, 6), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(30, 6), nullable=False),
        sa.Column("snapshot_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_position_snapshots_user_snapshot_at",
        "position_snapshots",
        ["user_id", "snapshot_at"],
    )
    op.create_index(
        "ix_position_snapshots_user_market_token_snapshot_at",
        "position_snapshots",
        ["user_id", "market_id", "token_id", "snapshot_at"],
    )

    op.create_table(
        "balances",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("asset", sa.String(length=128), nullable=False),
        sa.Column("balance", sa.Numeric(30, 6), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("user_id", "asset", name="pk_balances"),
    )

    op.create_table(
        "risk_state",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("kill_switch_on", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("kill_switch_reason", sa.String(length=255), nullable=True),
        sa.Column("daily_realized_pnl", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column("daily_peak_pnl", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column("daily_drawdown", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column(
            "last_reset_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    op.create_table(
        "audit_log",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("actor", sa.String(length=16), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("before", postgresql.JSONB(), nullable=True),
        sa.Column("after", postgresql.JSONB(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("actor IN ('user', 'system', 'admin')", name="ck_audit_log_actor"),
    )
    op.create_index("ix_audit_log_user_created_at", "audit_log", ["user_id", "created_at"])
    op.create_index("ix_audit_log_action_created_at", "audit_log", ["action", "created_at"])

    op.create_table(
        "notifications",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=True),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "channel IN ('email', 'webhook', 'push')", name="ck_notifications_channel"
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'sent', 'failed')", name="ck_notifications_status"
        ),
    )
    op.create_index(
        "ix_notifications_user_created_at", "notifications", ["user_id", "created_at"]
    )
    op.create_index(
        "ix_notifications_status_created_at",
        "notifications",
        ["status", "created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )

    op.create_table(
        "redemptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("condition_id", sa.String(length=128), nullable=False),
        sa.Column("payout_usdc", sa.Numeric(30, 6), nullable=False),
        sa.Column("tx_hash", sa.String(length=128), nullable=False),
        sa.Column("redeemed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_redemptions_user_redeemed_at", "redemptions", ["user_id", "redeemed_at"]
    )
    op.create_index("uq_redemptions_tx_hash", "redemptions", ["tx_hash"], unique=True)

    op.execute(
        """
        CREATE OR REPLACE FUNCTION raise_append_only_violation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'table % is append-only (% blocked)', TG_TABLE_NAME, TG_OP
                USING ERRCODE = 'restrict_violation';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    for table in APPEND_ONLY_TABLES:
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only
            BEFORE UPDATE OR DELETE OR TRUNCATE ON {table}
            FOR EACH STATEMENT
            EXECUTE FUNCTION raise_append_only_violation();
            """
        )


def downgrade() -> None:
    for table in APPEND_ONLY_TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS {table}_append_only ON {table};")
    op.execute("DROP FUNCTION IF EXISTS raise_append_only_violation();")

    op.drop_index("uq_redemptions_tx_hash", table_name="redemptions")
    op.drop_index("ix_redemptions_user_redeemed_at", table_name="redemptions")
    op.drop_table("redemptions")

    op.drop_index("ix_notifications_status_created_at", table_name="notifications")
    op.drop_index("ix_notifications_user_created_at", table_name="notifications")
    op.drop_table("notifications")

    op.drop_index("ix_audit_log_action_created_at", table_name="audit_log")
    op.drop_index("ix_audit_log_user_created_at", table_name="audit_log")
    op.drop_table("audit_log")

    op.drop_table("risk_state")
    op.drop_table("balances")

    op.drop_index(
        "ix_position_snapshots_user_market_token_snapshot_at", table_name="position_snapshots"
    )
    op.drop_index("ix_position_snapshots_user_snapshot_at", table_name="position_snapshots")
    op.drop_table("position_snapshots")
    op.drop_table("positions")

    op.drop_index("uq_fills_clob_fill_id", table_name="fills")
    op.drop_index("ix_fills_order_id", table_name="fills")
    op.drop_index("ix_fills_user_filled_at", table_name="fills")
    op.drop_table("fills")

    op.drop_index("uq_orders_clob_order_id", table_name="orders")
    op.drop_index("ix_orders_intent_id", table_name="orders")
    op.drop_index("ix_orders_user_status", table_name="orders")
    op.drop_table("orders")

    op.drop_index("ix_intents_signal_id", table_name="intents")
    op.drop_index("ix_intents_user_decided_at", table_name="intents")
    op.drop_table("intents")

    op.drop_index("ix_signals_instance_emitted_at", table_name="signals")
    op.drop_index("ix_signals_user_emitted_at", table_name="signals")
    op.drop_table("signals")

    op.drop_index("ix_strategy_instances_user_strategy", table_name="strategy_instances")
    op.drop_index("ix_strategy_instances_user_status", table_name="strategy_instances")
    op.drop_table("strategy_instances")

    op.drop_table("subscriptions")

    op.drop_index("uq_user_secrets_user_scope", table_name="user_secrets")
    op.drop_table("user_secrets")

    op.drop_index("uq_user_wallets_user_address", table_name="user_wallets")
    op.drop_index("ix_user_wallets_user_id", table_name="user_wallets")
    op.drop_table("user_wallets")
