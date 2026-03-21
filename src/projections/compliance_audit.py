"""
src/projections/compliance_audit.py
=====================================
ComplianceAuditView — regulatory read model with temporal query support.

This is the most critical projection — the one a regulator queries
during examination. It must be:
  - Complete: every compliance event recorded
  - Traceable: every rule references its regulation version
  - Temporally queryable: state at any past timestamp (Step 3 of video demo)

Temporal query strategy:
  get_compliance_at(application_id, timestamp) replays compliance_audit_view
  rows up to the given timestamp, using snapshots to avoid full replay.
  Snapshot trigger: every 50 events per application.

SLO: lag < 2000ms
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

import asyncpg

from src.projections.base import Projection

if TYPE_CHECKING:
    from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

SUBSCRIBED_EVENTS = {
    "ComplianceCheckRequested",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "ComplianceClearanceIssued",
}

SNAPSHOT_EVERY_N_EVENTS = 50


class ComplianceAuditViewProjection(Projection):
    """
    Maintains compliance_audit_view and compliance_audit_snapshots tables.
    """

    def __init__(self, pool: asyncpg.Pool | None = None) -> None:
        self._pool = pool
        self._event_counts: dict[str, int] = {}  # application_id → event count since last snapshot

    @property
    def name(self) -> str:
        return "ComplianceAuditView"

    @property
    def subscribed_event_types(self) -> set[str]:
        return SUBSCRIBED_EVENTS

    async def handle(self, event: "StoredEvent", conn: asyncpg.Connection) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            await handler(event, conn)
            # Snapshot trigger: every N compliance events per application
            app_id = event.payload.get("application_id")
            if app_id:
                count = self._event_counts.get(app_id, 0) + 1
                self._event_counts[app_id] = count
                if count % SNAPSHOT_EVERY_N_EVENTS == 0:
                    await self._take_snapshot(app_id, event.global_position, event.recorded_at, conn)

    async def rebuild(self, conn: asyncpg.Connection) -> None:
        await conn.execute("TRUNCATE TABLE compliance_audit_view")
        await conn.execute("TRUNCATE TABLE compliance_audit_snapshots")
        self._event_counts.clear()
        logger.info("ComplianceAuditView tables truncated for rebuild")

    # -------------------------------------------------------------------------
    # EVENT HANDLERS
    # -------------------------------------------------------------------------

    async def _on_ComplianceCheckRequested(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        for rule_id in p.get("checks_required", []):
            await conn.execute(
                """
                INSERT INTO compliance_audit_view (
                    application_id, rule_id, rule_version,
                    regulation_set_version, status,
                    event_global_position, recorded_at
                ) VALUES ($1, $2, 'pending', $3, 'PENDING', $4, $5)
                ON CONFLICT DO NOTHING
                """,
                p["application_id"],
                rule_id,
                p.get("regulation_set_version"),
                event.global_position,
                event.recorded_at,
            )

    async def _on_ComplianceRulePassed(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        # Upsert — rule may or may not have a PENDING row already
        await conn.execute(
            """
            INSERT INTO compliance_audit_view (
                application_id, rule_id, rule_version,
                status, evidence_hash, evaluation_timestamp,
                event_global_position, recorded_at
            ) VALUES ($1, $2, $3, 'PASSED', $4, $5, $6, $7)
            ON CONFLICT (application_id, rule_id, event_global_position)
            DO UPDATE SET
                status               = 'PASSED',
                rule_version         = EXCLUDED.rule_version,
                evidence_hash        = EXCLUDED.evidence_hash,
                evaluation_timestamp = EXCLUDED.evaluation_timestamp,
                recorded_at          = EXCLUDED.recorded_at
            """,
            p["application_id"],
            p["rule_id"],
            p["rule_version"],
            p.get("evidence_hash"),
            p.get("evaluation_timestamp"),
            event.global_position,
            event.recorded_at,
        )

    async def _on_ComplianceRuleFailed(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            """
            INSERT INTO compliance_audit_view (
                application_id, rule_id, rule_version,
                status, failure_reason, remediation_required,
                evaluation_timestamp, event_global_position, recorded_at
            ) VALUES ($1, $2, $3, 'FAILED', $4, $5, $6, $7, $8)
            ON CONFLICT (application_id, rule_id, event_global_position)
            DO UPDATE SET
                status               = 'FAILED',
                rule_version         = EXCLUDED.rule_version,
                failure_reason       = EXCLUDED.failure_reason,
                remediation_required = EXCLUDED.remediation_required,
                evaluation_timestamp = EXCLUDED.evaluation_timestamp
            """,
            p["application_id"],
            p["rule_id"],
            p["rule_version"],
            p.get("failure_reason"),
            p.get("remediation_required", True),
            p.get("evaluation_timestamp"),
            event.global_position,
            event.recorded_at,
        )

    async def _on_ComplianceClearanceIssued(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        # Mark all rules for this application as part of cleared set
        await conn.execute(
            """
            INSERT INTO compliance_audit_view (
                application_id, rule_id, rule_version,
                status, event_global_position, recorded_at
            ) VALUES ($1, '__CLEARANCE__', $2, 'PASSED', $3, $4)
            ON CONFLICT DO NOTHING
            """,
            p["application_id"],
            p.get("regulation_set_version", "unknown"),
            event.global_position,
            event.recorded_at,
        )

    # -------------------------------------------------------------------------
    # SNAPSHOT MANAGEMENT
    # -------------------------------------------------------------------------

    async def _take_snapshot(
        self,
        application_id: str,
        global_position: int,
        timestamp: datetime,
        conn: asyncpg.Connection,
    ) -> None:
        """Take a point-in-time snapshot of compliance state for this application."""
        rows = await conn.fetch(
            """
            SELECT rule_id, rule_version, status, failure_reason,
                   evidence_hash, evaluation_timestamp, recorded_at
            FROM compliance_audit_view
            WHERE application_id = $1
            ORDER BY recorded_at ASC
            """,
            application_id,
        )
        snapshot_data = [dict(r) for r in rows]
        await conn.execute(
            """
            INSERT INTO compliance_audit_snapshots (
                application_id, snapshot_at_position,
                snapshot_at_timestamp, snapshot_data
            ) VALUES ($1, $2, $3, $4::jsonb)
            """,
            application_id,
            global_position,
            timestamp,
            json.dumps(snapshot_data, default=str),
        )
        logger.debug(
            "Snapshot taken for application %s at position %d",
            application_id, global_position,
        )

    # -------------------------------------------------------------------------
    # TEMPORAL QUERY INTERFACE (Step 3 of video demo)
    # -------------------------------------------------------------------------

    async def get_current_compliance(
        self, application_id: str, conn: asyncpg.Connection
    ) -> list[dict[str, Any]]:
        """Return current compliance state for an application."""
        rows = await conn.fetch(
            """
            SELECT rule_id, rule_version, regulation_set_version,
                   status, failure_reason, remediation_required,
                   evidence_hash, evaluation_timestamp, recorded_at
            FROM compliance_audit_view
            WHERE application_id = $1
            ORDER BY recorded_at ASC
            """,
            application_id,
        )
        return [dict(r) for r in rows]

    async def get_compliance_at(
        self,
        application_id: str,
        as_of: datetime,
        conn: asyncpg.Connection,
    ) -> list[dict[str, Any]]:
        """
        Return compliance state as it existed at a specific point in time.
        This is the temporal query capability required for regulatory examination.

        Strategy:
          1. Find the nearest snapshot before as_of timestamp
          2. If snapshot exists: start from snapshot data
          3. Apply any events between snapshot and as_of timestamp
          4. If no snapshot: query rows directly filtered by recorded_at <= as_of

        This avoids full replay on every temporal query.
        """
        # Try to find a snapshot at or before as_of
        snapshot_row = await conn.fetchrow(
            """
            SELECT snapshot_data, snapshot_at_timestamp, snapshot_at_position
            FROM compliance_audit_snapshots
            WHERE application_id = $1
              AND snapshot_at_timestamp <= $2
            ORDER BY snapshot_at_position DESC
            LIMIT 1
            """,
            application_id,
            as_of,
        )

        if snapshot_row:
            # Start from snapshot, then apply events after snapshot up to as_of
            raw = snapshot_row["snapshot_data"]
            snapshot_data = json.loads(raw) if isinstance(raw, str) else list(raw)
            snapshot_position = snapshot_row["snapshot_at_position"]

            # Get events after snapshot but before as_of
            additional_rows = await conn.fetch(
                """
                SELECT rule_id, rule_version, regulation_set_version,
                       status, failure_reason, remediation_required,
                       evidence_hash, evaluation_timestamp, recorded_at
                FROM compliance_audit_view
                WHERE application_id = $1
                  AND event_global_position > $2
                  AND recorded_at <= $3
                ORDER BY recorded_at ASC
                """,
                application_id,
                snapshot_position,
                as_of,
            )
            # Merge: snapshot data + additional events
            result = snapshot_data.copy()
            for row in additional_rows:
                result.append(dict(row))
            return result
        else:
            # No snapshot — query directly filtered by timestamp
            rows = await conn.fetch(
                """
                SELECT rule_id, rule_version, regulation_set_version,
                       status, failure_reason, remediation_required,
                       evidence_hash, evaluation_timestamp, recorded_at
                FROM compliance_audit_view
                WHERE application_id = $1
                  AND recorded_at <= $2
                ORDER BY recorded_at ASC
                """,
                application_id,
                as_of,
            )
            return [dict(r) for r in rows]

    async def get_projection_lag(self, conn: asyncpg.Connection) -> float:
        """
        Return lag in milliseconds between the latest event in the store
        and the latest event this projection has processed.
        """
        latest_event_time = await conn.fetchval(
            "SELECT MAX(recorded_at) FROM events"
        )
        latest_projection_time = await conn.fetchval(
            """
            SELECT MAX(recorded_at) FROM compliance_audit_view
            """
        )
        if not latest_event_time or not latest_projection_time:
            return 0.0

        lag = (latest_event_time - latest_projection_time).total_seconds() * 1000
        return max(0.0, round(lag, 2))

    async def rebuild_from_scratch(self, conn: asyncpg.Connection) -> None:
        """Public alias for rebuild() — used by MCP resource handler."""
        await self.rebuild(conn)
