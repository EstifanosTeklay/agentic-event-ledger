"""
src/event_store.py
==================
The EventStore: append-only, ACID-compliant, PostgreSQL-backed event store.

Core guarantees:
1. Append atomicity: events + outbox written in a single transaction.
2. Optimistic concurrency: expected_version enforced via SELECT FOR UPDATE.
3. Immutability: no UPDATE or DELETE operations on the events table.
4. Ordered reads: load_stream returns events in stream_position order.
5. Upcasting: all loaded events pass through the UpcasterRegistry transparently.

See DESIGN.md for schema justification and EventStoreDB comparison.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from src.db import get_pool
from src.models.events import (
    BaseEvent,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamMetadata,
    StreamNotFoundError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# UPCASTER REGISTRY (forward declaration — populated in src/upcasting/registry.py)
# =============================================================================

class _IdentityUpcasterRegistry:
    """
    No-op registry used before the real registry is wired in.
    Ensures EventStore works in Phase 1 tests without upcasting infrastructure.
    """
    def upcast(self, event: StoredEvent) -> StoredEvent:
        return event


_upcaster_registry: Any = _IdentityUpcasterRegistry()


def set_upcaster_registry(registry: Any) -> None:
    """Wire in the real UpcasterRegistry after it is initialised (Phase 4)."""
    global _upcaster_registry
    _upcaster_registry = registry


# =============================================================================
# EVENT STORE
# =============================================================================

class EventStore:
    """
    Async PostgreSQL-backed event store.

    Usage:
        store = EventStore()
        version = await store.append(
            stream_id="loan-ABC-001",
            events=[ApplicationSubmitted(...)],
            expected_version=-1,  # new stream
        )
    """

    def __init__(self, pool: asyncpg.Pool | None = None) -> None:
        self._pool = pool  # injected in tests; acquired lazily in production

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is not None:
            return self._pool
        return await get_pool()

    # -------------------------------------------------------------------------
    # WRITE PATH
    # -------------------------------------------------------------------------

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        """
        Atomically append events to a stream.

        Args:
            stream_id:        Target stream identifier (e.g. "loan-ABC-001").
            events:           Ordered list of domain events to append.
            expected_version: -1 for new stream; N for exact version check.
            correlation_id:   Business process correlation identifier.
            causation_id:     ID of the command or event that caused these events.

        Returns:
            New stream version (stream_position of the last appended event).

        Raises:
            OptimisticConcurrencyError: Stream version does not match expected_version.
            ValueError: events list is empty.
        """
        if not events:
            raise ValueError("Cannot append empty events list")

        pool = await self._get_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                # ── Step 1: Lock the stream row and read current version ──────
                # SELECT FOR UPDATE serialises concurrent appenders on this stream.
                # Only one transaction can hold this lock at a time.
                # The other waits, then reads the updated version and fails the check.
                if expected_version == -1:
                    # New stream: insert into event_streams, expecting no prior row.
                    aggregate_type = _infer_aggregate_type(stream_id)
                    try:
                        await conn.execute(
                            """
                            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                            VALUES ($1, $2, 0)
                            """,
                            stream_id,
                            aggregate_type,
                        )
                        current_version = 0
                    except asyncpg.UniqueViolationError:
                        # Stream already exists — treat as version=0 and re-check
                        row = await conn.fetchrow(
                            "SELECT current_version FROM event_streams "
                            "WHERE stream_id = $1 FOR UPDATE",
                            stream_id,
                        )
                        current_version = row["current_version"]
                        if current_version != 0:
                            raise OptimisticConcurrencyError(
                                stream_id=stream_id,
                                expected_version=expected_version,
                                actual_version=current_version,
                            )
                else:
                    row = await conn.fetchrow(
                        "SELECT current_version FROM event_streams "
                        "WHERE stream_id = $1 FOR UPDATE",
                        stream_id,
                    )
                    if row is None:
                        raise StreamNotFoundError(stream_id)
                    current_version = row["current_version"]

                    if current_version != expected_version:
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected_version=expected_version,
                            actual_version=current_version,
                        )

                # ── Step 2: Insert events ─────────────────────────────────────
                metadata_base: dict[str, Any] = {}
                if correlation_id:
                    metadata_base["correlation_id"] = correlation_id
                if causation_id:
                    metadata_base["causation_id"] = causation_id

                new_version = current_version
                inserted_event_ids: list[UUID] = []

                for i, event in enumerate(events):
                    new_version = current_version + i + 1
                    payload = event.get_payload()
                    metadata = {**metadata_base, "schema_version": event.event_version}

                    row = await conn.fetchrow(
                        """
                        INSERT INTO events
                            (stream_id, stream_position, event_type, event_version, payload, metadata)
                        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
                        RETURNING event_id
                        """,
                        stream_id,
                        new_version,
                        event.event_type,
                        event.event_version,
                        json.dumps(payload, default=str),
                        json.dumps(metadata, default=str),
                    )
                    inserted_event_ids.append(row["event_id"])

                # ── Step 3: Update stream version ─────────────────────────────
                await conn.execute(
                    "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                    new_version,
                    stream_id,
                )

                # ── Step 4: Write outbox entries (same transaction) ───────────
                for event_id in inserted_event_ids:
                    await conn.execute(
                        """
                        INSERT INTO outbox (event_id, destination, payload)
                        VALUES ($1, $2, $3::jsonb)
                        """,
                        event_id,
                        "default",
                        json.dumps({"stream_id": stream_id, "event_id": str(event_id)}),
                    )

                logger.debug(
                    "Appended %d event(s) to stream '%s' → version %d",
                    len(events),
                    stream_id,
                    new_version,
                )
                return new_version

    # -------------------------------------------------------------------------
    # READ PATH
    # -------------------------------------------------------------------------

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Load all events for a stream in order, with upcasting applied.

        Args:
            stream_id:     Target stream.
            from_position: Start from this stream_position (inclusive). Default 0.
            to_position:   Stop at this stream_position (inclusive). Default: all.

        Returns:
            List of StoredEvent in ascending stream_position order, upcasted.

        Raises:
            StreamNotFoundError: stream_id does not exist in event_streams.
        """
        pool = await self._get_pool()

        async with pool.acquire() as conn:
            # Verify stream exists
            exists = await conn.fetchval(
                "SELECT 1 FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            if not exists:
                raise StreamNotFoundError(stream_id)

            if to_position is not None:
                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE stream_id = $1
                      AND stream_position >= $2
                      AND stream_position <= $3
                    ORDER BY stream_position ASC
                    """,
                    stream_id,
                    from_position,
                    to_position,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE stream_id = $1
                      AND stream_position >= $2
                    ORDER BY stream_position ASC
                    """,
                    stream_id,
                    from_position,
                )

        events = [_row_to_stored_event(row) for row in rows]
        # Apply upcasting transparently
        return [_upcaster_registry.upcast(e) for e in events]

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        """
        Async generator yielding all events from a global position.
        Used by the ProjectionDaemon for catch-up processing.

        Args:
            from_global_position: Resume from this global_position (exclusive).
            event_types:          If provided, filter to only these event types.
            batch_size:           Number of rows per DB fetch.

        Yields:
            StoredEvent in ascending global_position order, upcasted.
        """
        pool = await self._get_pool()
        cursor_position = from_global_position

        while True:
            async with pool.acquire() as conn:
                if event_types:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                          AND event_type = ANY($2::text[])
                        ORDER BY global_position ASC
                        LIMIT $3
                        """,
                        cursor_position,
                        event_types,
                        batch_size,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                        ORDER BY global_position ASC
                        LIMIT $2
                        """,
                        cursor_position,
                        batch_size,
                    )

            if not rows:
                return

            for row in rows:
                event = _row_to_stored_event(row)
                yield _upcaster_registry.upcast(event)

            cursor_position = rows[-1]["global_position"]

            if len(rows) < batch_size:
                return  # no more events

    # -------------------------------------------------------------------------
    # METADATA & ADMINISTRATION
    # -------------------------------------------------------------------------

    async def stream_version(self, stream_id: str) -> int:
        """
        Return the current version of a stream.

        Raises:
            StreamNotFoundError: stream_id not found.
        """
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        if row is None:
            raise StreamNotFoundError(stream_id)
        return row["current_version"]

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """
        Return metadata for a stream.

        Raises:
            StreamNotFoundError: stream_id not found.
        """
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version,
                       created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
        if row is None:
            raise StreamNotFoundError(stream_id)
        return StreamMetadata(
            stream_id=row["stream_id"],
            aggregate_type=row["aggregate_type"],
            current_version=row["current_version"],
            created_at=row["created_at"],
            archived_at=row["archived_at"],
            metadata=json.loads(row["metadata"]) if isinstance(row["metadata"], str) else dict(row["metadata"]),
        )

    async def archive_stream(self, stream_id: str) -> None:
        """
        Mark a stream as archived. Archived streams are read-only.
        The ProjectionDaemon skips events from archived streams.

        Raises:
            StreamNotFoundError: stream_id not found.
        """
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = NOW()
                WHERE stream_id = $1 AND archived_at IS NULL
                """,
                stream_id,
            )
        if result == "UPDATE 0":
            # Either not found or already archived — check which
            exists = await self._stream_exists(stream_id)
            if not exists:
                raise StreamNotFoundError(stream_id)
            logger.info("Stream '%s' was already archived", stream_id)

    async def get_latest_global_position(self) -> int:
        """Return the current maximum global_position in the events table."""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT COALESCE(MAX(global_position), 0) FROM events"
            )
        return result or 0

    # -------------------------------------------------------------------------
    # PRIVATE HELPERS
    # -------------------------------------------------------------------------

    async def _stream_exists(self, stream_id: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT 1 FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        return result is not None


# =============================================================================
# HELPERS
# =============================================================================

def _row_to_stored_event(row: asyncpg.Record) -> StoredEvent:
    """Convert a raw asyncpg row to a StoredEvent."""
    payload = row["payload"]
    metadata = row["metadata"]

    # asyncpg returns JSONB as dict directly; handle both cases
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(metadata, str):
        metadata = json.loads(metadata)

    return StoredEvent(
        event_id=row["event_id"],
        stream_id=row["stream_id"],
        stream_position=row["stream_position"],
        global_position=row["global_position"],
        event_type=row["event_type"],
        event_version=row["event_version"],
        payload=dict(payload),
        metadata=dict(metadata),
        recorded_at=row["recorded_at"],
    )


def _infer_aggregate_type(stream_id: str) -> str:
    """
    Infer aggregate type from stream_id prefix convention.
    e.g. "loan-ABC" → "LoanApplication"
         "agent-X-Y" → "AgentSession"
    """
    prefix_map = {
        "loan-":       "LoanApplication",
        "agent-":      "AgentSession",
        "compliance-": "ComplianceRecord",
        "audit-":      "AuditLedger",
    }
    for prefix, agg_type in prefix_map.items():
        if stream_id.startswith(prefix):
            return agg_type
    return "Unknown"
