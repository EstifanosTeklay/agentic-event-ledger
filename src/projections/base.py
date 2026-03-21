"""
src/projections/base.py
========================
Base class for all projections.

Every projection:
  - Has a unique name (used as checkpoint key)
  - Declares which event types it handles
  - Implements handle(event) — idempotent, no side effects on duplicate
  - Receives a DB connection from the daemon (no pool management here)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg
    from src.models.events import StoredEvent


class Projection(ABC):
    """
    Abstract base for all read-model projections.

    The daemon calls handle() for every event whose type is in
    subscribed_event_types. Handlers must be idempotent — the daemon
    uses at-least-once delivery (checkpoint may lag on crash).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique projection name — used as projection_checkpoints key."""
        ...

    @property
    @abstractmethod
    def subscribed_event_types(self) -> set[str]:
        """Set of event type strings this projection handles. Empty = all."""
        ...

    @abstractmethod
    async def handle(self, event: "StoredEvent", conn: "asyncpg.Connection") -> None:
        """
        Process one event and update the read model.
        Called within a transaction managed by the daemon.
        Must be idempotent — safe to call twice with the same event.
        """
        ...

    async def rebuild(self, conn: "asyncpg.Connection") -> None:
        """
        Optional: truncate and rebuild this projection's table from scratch.
        Default implementation does nothing — override in projections that
        support rebuild_from_scratch().
        """
        pass
