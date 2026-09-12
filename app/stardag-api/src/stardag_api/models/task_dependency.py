"""TaskDependency model for graph edges."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text

from stardag_api.models.base import Base, TimestampMixin, generate_uuid7

if TYPE_CHECKING:
    from stardag_api.models.task import Task


class TaskDependency(Base, TimestampMixin):
    """Graph edges representing task dependencies.

    upstream_task_id -> downstream_task_id means:
    "downstream depends on upstream" or "upstream must complete before downstream"

    Supports efficient graph traversal queries for:
    - Finding all upstream dependencies (what does this task depend on?)
    - Finding all downstream dependents (what depends on this task?)
    - Full DAG visualization
    """

    __tablename__ = "task_dependencies"
    __table_args__ = (
        UniqueConstraint(
            "upstream_task_id",
            "downstream_task_id",
            name="uq_task_dependency_edge",
        ),
        Index("ix_task_dep_upstream", "upstream_task_id"),
        Index("ix_task_dep_downstream", "downstream_task_id"),
        # Scheduling reads only current edges, and reads them constantly:
        # the frontier's ``has_incomplete_upstream`` is a correlated EXISTS
        # evaluated per candidate task, on a frontier re-read every few
        # seconds per active build. Partial so the index holds only the rows
        # those queries can match; the full-column index above still serves
        # the DAG view, which deliberately shows retracted edges too.
        Index(
            "ix_task_dep_downstream_current",
            "downstream_task_id",
            postgresql_where=text("superseded_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=generate_uuid7,
    )

    upstream_task_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    downstream_task_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )

    # True when this edge was added at runtime because the downstream task
    # yielded the upstream as a dynamic dep. False for edges coming from a
    # task's static ``requires()`` at registration time. An edge that exists
    # as both static and dynamic (unusual but possible) is stored once with
    # the FIRST observation; we don't flip from False -> True on later writes
    # because the initial registration is authoritative.
    is_dynamic: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="false",
        default=False,
    )

    # When this edge stopped describing how the downstream task is built.
    # NULL is the ordinary state: the edge is current and gates.
    #
    # **A dependency edge is evidence asserted by an act**, and it stops
    # counting when its source is withdrawn. For a dynamic edge the act is
    # one execution attempt of the downstream task: it yielded this upstream
    # and suspended. Abandon the attempt — reset the task to PENDING, or take
    # its suspension over from another build — and the generation it yielded
    # is no longer what the task needs, so the edges are superseded here.
    #
    # Without that the set only ever grew (edges are written ON CONFLICT DO
    # NOTHING), and an attempt abandoned with incomplete children left them
    # gating their own parent forever: it could not be scheduled until they
    # completed, so the next build reset and ran a whole stale generation
    # before the task ever re-yielded.
    #
    # Superseded rather than deleted, because the edge remains true history
    # — that attempt really did need those tasks — and the DAG view shows it.
    superseded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    upstream_task: Mapped[Task] = relationship(
        foreign_keys=[upstream_task_id],
        back_populates="downstream_edges",
    )
    downstream_task: Mapped[Task] = relationship(
        foreign_keys=[downstream_task_id],
        back_populates="upstream_edges",
    )
