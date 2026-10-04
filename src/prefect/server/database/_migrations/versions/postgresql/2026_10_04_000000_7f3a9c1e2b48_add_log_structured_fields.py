"""Add `structured_fields` column to log

Revision ID: 7f3a9c1e2b48
Revises: c8d5f2a71b3e
Create Date: 2026-10-04 00:00:00.000000

Carries caller-provided structured key/value pairs (the standard library
`extra=` attributes on a log record) from clients that have structured log
fields enabled. The column is nullable and has no server default, so rows
written by older clients are stored unchanged.
"""

import sqlalchemy as sa
from alembic import op

import prefect

# revision identifiers, used by Alembic.
revision = "7f3a9c1e2b48"
down_revision = "c8d5f2a71b3e"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "log",
        sa.Column(
            "structured_fields",
            prefect.server.utilities.database.JSON(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("log", "structured_fields")
