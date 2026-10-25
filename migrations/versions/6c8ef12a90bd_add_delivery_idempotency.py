"""Add delivery creation idempotency.

Revision ID: 6c8ef12a90bd
Revises: 2f7dc2ea32fa
"""

import sqlalchemy as sa
from alembic import op

revision: str = "6c8ef12a90bd"
down_revision: str = "2f7dc2ea32fa"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.add_column(
        "webhook_deliveries",
        sa.Column("idempotency_key", sa.String(255), nullable=True),
    )
    op.add_column(
        "webhook_deliveries",
        sa.Column("request_fingerprint", sa.String(64), nullable=True),
    )
    op.create_unique_constraint(
        op.f("uq_webhook_deliveries_idempotency_key"),
        "webhook_deliveries",
        ["idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("uq_webhook_deliveries_idempotency_key"),
        "webhook_deliveries",
        type_="unique",
    )
    op.drop_column("webhook_deliveries", "request_fingerprint")
    op.drop_column("webhook_deliveries", "idempotency_key")
