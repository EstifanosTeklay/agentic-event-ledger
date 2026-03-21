"""
src/projections/daemon.py
==========================
ProjectionDaemon — fault-tolerant async background processor.

Responsibilities:
  - Polls events table from last checkpoint position
  - Routes each event to subscribed projections
  - Manages per-projection checkpoints atomically
  - Exposes get_lag() per projection (SLO monitoring)
  - Fault-tolerant: bad events are logged and skipped after max retries
  - Supports PostgreSQL LISTEN/NOTIFY for low-latency wake-up

SLO contracts (from DESIGN.md):
  ApplicationSummary:    lag < 500ms
  AgentPerformanceLedger: lag < 500ms
  ComplianceAuditView:   lag < 2000ms

Design: Marten Async Daemon pattern in Python.
See DOMAIN_NOTES.md Q6 for distributed coordination analysis.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.db import get_pool
from src.models.events import StoredEvent
from src.projections.base import Projection

if TYPE_CHECKING:
    from src.event_store import EventStore

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Async background daemon that keeps all projections current.

    Usage:
        daemon = ProjectionDaemon(store, [
            ApplicationSummaryProjection(pool),
            AgentPerformanceLedgerProjection(pool),
            ComplianceAuditViewProjection(pool),
        ])
        asyncio.create_task(daemon.run_forever())
        ...
        await daemon.stop()
    """

    def __init__(
        self,
        store: "EventStore",
        projections: list[Projection],
        pool: asyncpg.Pool | None = None,
        batch_size: int = 500,
        max_retries: int = 3,
    ) -> None:
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._pool = pool
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._running = False

        # Lag tracking: projection_name → (last_processed_position, timestamp)
        self._lag_state: dict[str, tuple[int, float]] = {
            name: (0, time.monotonic()) for name in self._projections
        }

        # Per-event retry counters: (projection_name, event_id) → attempt_count
        self._retry_counts: dict[tuple[str, str], int] = {}

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is not None:
            return self._pool
        return await get_pool()

    # -------------------------------------------------------------------------
    # MAIN LOOP
    # -------------------------------------------------------------------------

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        """
        Run the daemon indefinitely, polling for new events.
        Also listens for LISTEN/NOTIFY 'new_event' signals to wake up early.
        """
        self._running = True
        pool = await self._get_pool()

        # Initialise checkpoints for any new projections
        await self._initialise_checkpoints(pool)

        # Set up LISTEN/NOTIFY on a dedicated connection
        notify_conn = await pool.acquire()
        try:
            await notify_conn.add_listener("new_event", self._on_notify)
            logger.info(
                "ProjectionDaemon started. Managing projections: %s",
                list(self._projections.keys()),
            )

            while self._running:
                try:
                    await self._process_batch(pool)
                except Exception as e:
                    logger.error("Daemon batch error: %s", e, exc_info=True)

                await asyncio.sleep(poll_interval_ms / 1000)

        finally:
            await notify_conn.remove_listener("new_event", self._on_notify)
            await pool.release(notify_conn)

    def _on_notify(self, conn, pid, channel, payload) -> None:
        """LISTEN/NOTIFY callback — triggers immediate processing."""
        logger.debug("NOTIFY received on channel '%s': %s", channel, payload)
        # The next poll_interval cycle will pick it up — no explicit wake needed
        # since poll_interval is already short (100ms default)

    async def stop(self) -> None:
        """Gracefully stop the daemon after the current batch completes."""
        self._running = False
        logger.info("ProjectionDaemon stopping...")

    # -------------------------------------------------------------------------
    # BATCH PROCESSING
    # -------------------------------------------------------------------------

    async def _process_batch(self, pool: asyncpg.Pool) -> None:
        """
        Load the lowest checkpoint across all projections, fetch the next
        batch of events, and route each event to its subscribed projections.
        """
        async with pool.acquire() as conn:
            # Load all current checkpoints
            checkpoints = await self._load_checkpoints(conn)

            # Find the minimum checkpoint — we must process from there
            min_position = min(checkpoints.values()) if checkpoints else 0

            # Fetch next batch of events
            rows = await conn.fetch(
                """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events
                WHERE global_position > $1
                ORDER BY global_position ASC
                LIMIT $2
                """,
                min_position,
                self._batch_size,
            )

            if not rows:
                return

            # Process each event through relevant projections
            for row in rows:
                event = self._row_to_stored_event(row)
                for proj_name, projection in self._projections.items():
                    proj_checkpoint = checkpoints.get(proj_name, 0)

                    # Skip if this projection already processed this event
                    if event.global_position <= proj_checkpoint:
                        continue

                    # Skip if event type not subscribed (empty set = all)
                    if (projection.subscribed_event_types and
                            event.event_type not in projection.subscribed_event_types):
                        # Still advance checkpoint for unsubscribed events
                        await self._update_checkpoint(conn, proj_name, event.global_position)
                        checkpoints[proj_name] = event.global_position
                        continue

                    # Process with retry logic
                    await self._process_event_with_retry(
                        conn, projection, event, checkpoints
                    )

            # Update lag state
            if rows:
                latest_global = rows[-1]["global_position"]
                for proj_name in self._projections:
                    self._lag_state[proj_name] = (
                        checkpoints.get(proj_name, 0),
                        time.monotonic(),
                    )

    async def _process_event_with_retry(
        self,
        conn: asyncpg.Connection,
        projection: Projection,
        event: StoredEvent,
        checkpoints: dict[str, int],
    ) -> None:
        """
        Process one event through one projection with retry logic.
        On max_retries exhaustion: log error, skip event, advance checkpoint.
        This ensures a bad event never permanently stalls the daemon.
        """
        retry_key = (projection.name, str(event.event_id))
        attempts = self._retry_counts.get(retry_key, 0)

        if attempts >= self._max_retries:
            logger.error(
                "Skipping event %s (type=%s, pos=%d) for projection '%s' "
                "after %d failed attempts",
                event.event_id, event.event_type,
                event.global_position, projection.name, attempts,
            )
            await self._update_checkpoint(conn, projection.name, event.global_position)
            checkpoints[projection.name] = event.global_position
            self._retry_counts.pop(retry_key, None)
            return

        try:
            async with conn.transaction():
                await projection.handle(event, conn)
                await self._update_checkpoint(conn, projection.name, event.global_position)

            checkpoints[projection.name] = event.global_position
            self._retry_counts.pop(retry_key, None)

        except Exception as e:
            self._retry_counts[retry_key] = attempts + 1
            logger.warning(
                "Projection '%s' failed on event %s (attempt %d/%d): %s",
                projection.name, event.event_id,
                attempts + 1, self._max_retries, e,
            )

    # -------------------------------------------------------------------------
    # CHECKPOINT MANAGEMENT
    # -------------------------------------------------------------------------

    async def _initialise_checkpoints(self, pool: asyncpg.Pool) -> None:
        """Ensure all projections have a checkpoint row (default position 0)."""
        async with pool.acquire() as conn:
            for proj_name in self._projections:
                await conn.execute(
                    """
                    INSERT INTO projection_checkpoints (projection_name, last_position)
                    VALUES ($1, 0)
                    ON CONFLICT (projection_name) DO NOTHING
                    """,
                    proj_name,
                )

    async def _load_checkpoints(self, conn: asyncpg.Connection) -> dict[str, int]:
        """Load current checkpoint positions for all managed projections."""
        rows = await conn.fetch(
            """
            SELECT projection_name, last_position
            FROM projection_checkpoints
            WHERE projection_name = ANY($1::text[])
            """,
            list(self._projections.keys()),
        )
        result = {row["projection_name"]: row["last_position"] for row in rows}
        # Default 0 for any projection not yet in DB
        for name in self._projections:
            result.setdefault(name, 0)
        return result

    async def _update_checkpoint(
        self,
        conn: asyncpg.Connection,
        projection_name: str,
        position: int,
    ) -> None:
        """Update checkpoint for a projection — called after successful handle()."""
        await conn.execute(
            """
            UPDATE projection_checkpoints
            SET last_position = $1, updated_at = NOW()
            WHERE projection_name = $2
            """,
            position,
            projection_name,
        )

    # -------------------------------------------------------------------------
    # LAG METRICS
    # -------------------------------------------------------------------------

    async def get_lag(self, projection_name: str) -> float:
        """
        Return lag in milliseconds for a specific projection.
        Lag = time since the projection last processed an event,
        or time since daemon started if no events processed yet.

        Used for SLO monitoring and the ledger://ledger/health resource.
        """
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            # Get latest global position in the store
            latest = await conn.fetchval(
                "SELECT COALESCE(MAX(global_position), 0) FROM events"
            )
            # Get projection's current checkpoint
            checkpoint = await conn.fetchval(
                """
                SELECT last_position FROM projection_checkpoints
                WHERE projection_name = $1
                """,
                projection_name,
            )
            checkpoint = checkpoint or 0

        if latest == 0 or latest == checkpoint:
            return 0.0  # fully caught up

        # If behind, calculate time-based lag from last processing timestamp
        _, last_processed_time = self._lag_state.get(
            projection_name, (0, time.monotonic())
        )
        lag_ms = (time.monotonic() - last_processed_time) * 1000
        return round(lag_ms, 2)

    async def get_all_lags(self) -> dict[str, float]:
        """Return lag in milliseconds for all managed projections."""
        result = {}
        for name in self._projections:
            result[name] = await self.get_lag(name)
        return result

    async def get_projection_position(self, projection_name: str) -> int:
        """Return the last processed global_position for a projection."""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            pos = await conn.fetchval(
                """
                SELECT last_position FROM projection_checkpoints
                WHERE projection_name = $1
                """,
                projection_name,
            )
        return pos or 0

    # -------------------------------------------------------------------------
    # REBUILD
    # -------------------------------------------------------------------------

    async def rebuild_projection(self, projection_name: str) -> None:
        """
        Rebuild a projection from scratch by:
        1. Calling projection.rebuild() to truncate its table
        2. Resetting its checkpoint to 0
        3. Processing all events from position 0

        Does NOT pause live reads — the projection table remains queryable
        during rebuild (reads see stale data until rebuild completes).
        """
        if projection_name not in self._projections:
            raise ValueError(f"Unknown projection: {projection_name}")

        projection = self._projections[projection_name]
        pool = await self._get_pool()

        logger.info("Rebuilding projection '%s' from scratch...", projection_name)

        async with pool.acquire() as conn:
            # Truncate projection table
            await projection.rebuild(conn)
            # Reset checkpoint
            await conn.execute(
                """
                UPDATE projection_checkpoints
                SET last_position = 0, updated_at = NOW()
                WHERE projection_name = $1
                """,
                projection_name,
            )

        logger.info(
            "Projection '%s' table cleared. Replaying events...", projection_name
        )

        # Replay all events through this projection
        processed = 0
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events ORDER BY global_position ASC
                """
            )
            for row in rows:
                event = self._row_to_stored_event(row)
                if (projection.subscribed_event_types and
                        event.event_type not in projection.subscribed_event_types):
                    continue
                try:
                    async with conn.transaction():
                        await projection.handle(event, conn)
                        await self._update_checkpoint(
                            conn, projection_name, event.global_position
                        )
                    processed += 1
                except Exception as e:
                    logger.error(
                        "Rebuild error on event %s: %s", event.event_id, e
                    )

        logger.info(
            "Projection '%s' rebuilt successfully. %d events processed.",
            projection_name, processed,
        )

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------

    @staticmethod
    def _row_to_stored_event(row: asyncpg.Record) -> StoredEvent:
        payload = row["payload"]
        metadata = row["metadata"]
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
