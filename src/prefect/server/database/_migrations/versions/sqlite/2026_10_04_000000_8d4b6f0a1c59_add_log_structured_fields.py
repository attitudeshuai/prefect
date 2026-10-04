"""Add `structured_fields` column to log

Revision ID: 8d4b6f0a1c59
Revises: f416ea180ae1
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
revision = "8d4b6f0a1c59"
down_revision = "f416ea180ae1"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "structured_fields",
                prefect.server.utilities.database.JSON(astext_type=sa.Text()),
                nullable=True,
            )
        )


def downgrade():
    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.drop_column("structured_fields")
