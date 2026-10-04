"""Add retry budget columns to flow_run and task_run

Revision ID: f8a9b0c1d2e3
Revises: c8d5f2a71b3e
Create Date: 2026-10-05 00:00:00.000000

Adds the persisted state for the cross-attempt cumulative run-time budget:

- ``retry_budget_elapsed``: cumulative elapsed time counted against the budget,
  folded in as running (and optionally retry-wait) segments close.
- ``retry_budget_wait_state_id``: idempotency guard while awaiting a retry.
- ``retry_budget_count_wait``: wait-inclusion basis frozen at AwaitingRetry entry.
- ``retry_budget_exceeded``: sticky marker set once the budget is exceeded.

See the SQLite counterpart for the other dialect.
"""

import sqlalchemy as sa
from alembic import op

import prefect

# revision identifiers, used by Alembic.
revision = "f8a9b0c1d2e3"
down_revision = "c8d5f2a71b3e"
branch_labels = None
depends_on = None


def _new_columns() -> list[sa.Column]:
    return [
        sa.Column(
            "retry_budget_elapsed",
            sa.Interval(),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "retry_budget_wait_state_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column("retry_budget_count_wait", sa.Boolean(), nullable=True),
        sa.Column(
            "retry_budget_exceeded",
            sa.Boolean(),
            server_default="0",
            nullable=False,
        ),
    ]


def upgrade():
    for table in ("flow_run", "task_run"):
        for column in _new_columns():
            op.add_column(table, column)


def downgrade():
    for table in ("flow_run", "task_run"):
        op.drop_column(table, "retry_budget_exceeded")
        op.drop_column(table, "retry_budget_count_wait")
        op.drop_column(table, "retry_budget_wait_state_id")
        op.drop_column(table, "retry_budget_elapsed")
