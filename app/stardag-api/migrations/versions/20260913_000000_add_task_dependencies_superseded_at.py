"""add task_dependencies.superseded_at

A dependency edge is evidence asserted by an act, and until now no act
could withdraw one. Dynamic edges are written ON CONFLICT DO NOTHING, so a
task's set only ever grew across attempts: an attempt abandoned with its
children incomplete left them gating their own parent forever, and the next
build to want that task reset and re-ran the whole stale generation before
it could re-yield. This column is what a retraction writes.

**Additive and NULL-defaulting on purpose.** Every existing row means
"current", which is exactly what it meant before the column existed, so the
upgrade changes no scheduling decision anywhere. Nothing is backfilled and
nothing needs to be: retraction is a forward-looking rule, and inferring
retroactively which historical edges "would have been" superseded would be
guessing at executions nobody recorded.

The partial index is the load-bearing half. The frontier's
``has_incomplete_upstream`` is a correlated EXISTS evaluated per candidate
task, on a frontier that is re-read every few seconds for every active
build, and it now carries an extra predicate. ``ix_task_dep_downstream``
stays: the DAG view reads every edge, retracted ones included, and would
otherwise lose its index.

Created CONCURRENTLY is deliberately NOT used. The table is small (one row
per edge, and edges are only written at registration and at dynamic-dep
yields), and a concurrent index cannot run inside the transaction Alembic
wraps each migration in — the cost of the brief lock is far below the cost
of a migration that can leave an INVALID index behind on failure.

Revision ID: 3f1a7c5d9e21
Revises: b41c7d9e2f08
Create Date: 2026-09-13 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "3f1a7c5d9e21"
down_revision: Union[str, Sequence[str], None] = "b41c7d9e2f08"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "task_dependencies",
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_task_dep_downstream_current",
        "task_dependencies",
        ["downstream_task_id"],
        postgresql_where=sa.text("superseded_at IS NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_task_dep_downstream_current", table_name="task_dependencies")
    op.drop_column("task_dependencies", "superseded_at")
