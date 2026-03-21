"""
src/projections/agent_performance.py
======================================
AgentPerformanceLedger — aggregated metrics per AI agent model version.

Answers: "Has agent v2.3 been making systematically different decisions
than v2.2?" — the key governance question for model drift detection.

SLO: lag < 500ms
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import asyncpg

from src.projections.base import Projection

if TYPE_CHECKING:
    from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

SUBSCRIBED_EVENTS = {
    "CreditAnalysisCompleted",
    "DecisionGenerated",
    "HumanReviewCompleted",
    "AgentContextLoaded",
}


class AgentPerformanceLedgerProjection(Projection):
    """
    Maintains the agent_performance_ledger table.
    One row per (agent_id, model_version) pair.
    All aggregations use running averages to avoid full table scans.
    """

    @property
    def name(self) -> str:
        return "AgentPerformanceLedger"

    @property
    def subscribed_event_types(self) -> set[str]:
        return SUBSCRIBED_EVENTS

    async def handle(self, event: "StoredEvent", conn: asyncpg.Connection) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            await handler(event, conn)

    async def rebuild(self, conn: asyncpg.Connection) -> None:
        await conn.execute("TRUNCATE TABLE agent_performance_ledger")
        logger.info("AgentPerformanceLedger table truncated for rebuild")

    async def _ensure_row(
        self, agent_id: str, model_version: str, conn: asyncpg.Connection
    ) -> None:
        """Upsert a row for this agent/model_version if it doesn't exist."""
        await conn.execute(
            """
            INSERT INTO agent_performance_ledger (agent_id, model_version)
            VALUES ($1, $2)
            ON CONFLICT (agent_id, model_version) DO NOTHING
            """,
            agent_id,
            model_version,
        )

    async def _on_CreditAnalysisCompleted(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        agent_id = p["agent_id"]
        model_version = p.get("model_version", "unknown")
        confidence = p.get("confidence_score")
        duration_ms = p.get("analysis_duration_ms", 0)

        await self._ensure_row(agent_id, model_version, conn)

        # Running average for confidence and duration
        await conn.execute(
            """
            UPDATE agent_performance_ledger SET
                analyses_completed = analyses_completed + 1,
                avg_confidence_score = CASE
                    WHEN $1::float IS NULL THEN avg_confidence_score
                    WHEN avg_confidence_score IS NULL THEN $1::float
                    ELSE (avg_confidence_score * analyses_completed + $1::float)
                         / (analyses_completed + 1)
                END,
                avg_duration_ms = CASE
                    WHEN avg_duration_ms IS NULL THEN $2::numeric
                    ELSE (avg_duration_ms * analyses_completed + $2::numeric)
                         / (analyses_completed + 1)
                END,
                last_seen_at = $3,
                updated_at   = NOW()
            WHERE agent_id = $4 AND model_version = $5
            """,
            confidence,
            duration_ms,
            event.recorded_at,
            agent_id,
            model_version,
        )

    async def _on_DecisionGenerated(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        agent_id = p["orchestrator_agent_id"]
        model_versions = p.get("model_versions", {})
        model_version = model_versions.get("orchestrator", "unknown")
        recommendation = p.get("recommendation", "")

        await self._ensure_row(agent_id, model_version, conn)

        # Increment decision count and update rates
        await conn.execute(
            """
            UPDATE agent_performance_ledger SET
                decisions_generated = decisions_generated + 1,
                approve_rate = CASE
                    WHEN $1 = 'APPROVE'
                    THEN (COALESCE(approve_rate, 0) * decisions_generated + 1.0)
                         / (decisions_generated + 1)
                    ELSE COALESCE(approve_rate, 0) * decisions_generated
                         / GREATEST(decisions_generated + 1, 1)
                END,
                decline_rate = CASE
                    WHEN $1 = 'DECLINE'
                    THEN (COALESCE(decline_rate, 0) * decisions_generated + 1.0)
                         / (decisions_generated + 1)
                    ELSE COALESCE(decline_rate, 0) * decisions_generated
                         / GREATEST(decisions_generated + 1, 1)
                END,
                refer_rate = CASE
                    WHEN $1 = 'REFER'
                    THEN (COALESCE(refer_rate, 0) * decisions_generated + 1.0)
                         / (decisions_generated + 1)
                    ELSE COALESCE(refer_rate, 0) * decisions_generated
                         / GREATEST(decisions_generated + 1, 1)
                END,
                last_seen_at = $2,
                updated_at   = NOW()
            WHERE agent_id = $3 AND model_version = $4
            """,
            recommendation,
            event.recorded_at,
            agent_id,
            model_version,
        )

    async def _on_HumanReviewCompleted(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        """Track human override rate — key governance metric."""
        if not event.payload.get("override"):
            return

        # Find the orchestrator agent for this application to attribute override
        app_id = event.payload["application_id"]
        row = await conn.fetchrow(
            """
            SELECT agent_id, model_version, decisions_generated
            FROM agent_performance_ledger
            WHERE decisions_generated > 0
            ORDER BY last_seen_at DESC
            LIMIT 1
            """
        )
        if not row:
            return

        await conn.execute(
            """
            UPDATE agent_performance_ledger SET
                human_override_rate = CASE
                    WHEN decisions_generated = 0 THEN 0
                    ELSE (COALESCE(human_override_rate, 0) * decisions_generated + 1.0)
                         / decisions_generated
                END,
                updated_at = NOW()
            WHERE agent_id = $1 AND model_version = $2
            """,
            row["agent_id"],
            row["model_version"],
        )

    async def _on_AgentContextLoaded(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        """Register a new agent/model_version pair when a session starts."""
        p = event.payload
        await self._ensure_row(p["agent_id"], p["model_version"], conn)
        await conn.execute(
            """
            UPDATE agent_performance_ledger SET
                first_seen_at = LEAST(first_seen_at, $1),
                last_seen_at  = GREATEST(last_seen_at, $1),
                updated_at    = NOW()
            WHERE agent_id = $2 AND model_version = $3
            """,
            event.recorded_at,
            p["agent_id"],
            p["model_version"],
        )
