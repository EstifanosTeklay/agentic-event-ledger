"""
src/projections/application_summary.py
========================================
ApplicationSummary — current state of every loan application.

One row per application. Updated by the daemon as events arrive.
SLO: lag < 500ms (enforced in test_projections.py).

All reads from ledger://applications/{id} hit this table — never the
event stream directly. This is the CQRS read model contract.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import asyncpg

from src.projections.base import Projection

if TYPE_CHECKING:
    from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

# Event types this projection subscribes to
SUBSCRIBED_EVENTS = {
    "ApplicationSubmitted",
    "CreditAnalysisRequested",
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckRequested",
    "ComplianceClearanceIssued",
    "DecisionGenerated",
    "HumanReviewCompleted",
    "ApplicationApproved",
    "ApplicationDeclined",
    "ApplicationUnderReview",
}


class ApplicationSummaryProjection(Projection):
    """
    Maintains the application_summary table.
    Uses INSERT ... ON CONFLICT DO UPDATE for idempotency.
    """

    @property
    def name(self) -> str:
        return "ApplicationSummary"

    @property
    def subscribed_event_types(self) -> set[str]:
        return SUBSCRIBED_EVENTS

    async def handle(self, event: "StoredEvent", conn: asyncpg.Connection) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            await handler(event, conn)

    async def rebuild(self, conn: asyncpg.Connection) -> None:
        await conn.execute("TRUNCATE TABLE application_summary")
        logger.info("ApplicationSummary table truncated for rebuild")

    # -------------------------------------------------------------------------
    # EVENT HANDLERS
    # -------------------------------------------------------------------------

    async def _on_ApplicationSubmitted(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary (
                application_id, state, applicant_id,
                requested_amount_usd, last_event_type, last_event_at, data_as_of
            ) VALUES ($1, 'SUBMITTED', $2, $3, $4, $5, $5)
            ON CONFLICT (application_id) DO UPDATE SET
                state            = 'SUBMITTED',
                applicant_id     = EXCLUDED.applicant_id,
                requested_amount_usd = EXCLUDED.requested_amount_usd,
                last_event_type  = EXCLUDED.last_event_type,
                last_event_at    = EXCLUDED.last_event_at,
                data_as_of       = EXCLUDED.data_as_of,
                updated_at       = NOW()
            """,
            event.payload["application_id"],
            event.payload["applicant_id"],
            event.payload["requested_amount_usd"],
            event.event_type,
            event.recorded_at,
        )

    async def _on_CreditAnalysisRequested(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                state           = 'AWAITING_ANALYSIS',
                last_event_type = $1,
                last_event_at   = $2,
                data_as_of      = $2,
                updated_at      = NOW()
            WHERE application_id = $3
            """,
            event.event_type,
            event.recorded_at,
            event.payload["application_id"],
        )

    async def _on_CreditAnalysisCompleted(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        # Load current agent_sessions_completed list
        row = await conn.fetchrow(
            "SELECT agent_sessions_completed FROM application_summary WHERE application_id = $1",
            p["application_id"],
        )
        sessions = []
        if row:
            raw = row["agent_sessions_completed"]
            sessions = json.loads(raw) if isinstance(raw, str) else list(raw)

        session_id = p.get("session_id")
        if session_id and session_id not in sessions:
            sessions.append(session_id)

        await conn.execute(
            """
            UPDATE application_summary SET
                state                    = 'ANALYSIS_COMPLETE',
                risk_tier                = $1,
                agent_sessions_completed = $2::jsonb,
                last_event_type          = $3,
                last_event_at            = $4,
                data_as_of               = $4,
                updated_at               = NOW()
            WHERE application_id = $5
            """,
            p.get("risk_tier"),
            json.dumps(sessions),
            event.event_type,
            event.recorded_at,
            p["application_id"],
        )

    async def _on_FraudScreeningCompleted(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                fraud_score     = $1,
                last_event_type = $2,
                last_event_at   = $3,
                data_as_of      = $3,
                updated_at      = NOW()
            WHERE application_id = $4
            """,
            event.payload["fraud_score"],
            event.event_type,
            event.recorded_at,
            event.payload["application_id"],
        )

    async def _on_ComplianceCheckRequested(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                state             = 'COMPLIANCE_REVIEW',
                compliance_status = 'PENDING',
                last_event_type   = $1,
                last_event_at     = $2,
                data_as_of        = $2,
                updated_at        = NOW()
            WHERE application_id = $3
            """,
            event.event_type,
            event.recorded_at,
            event.payload["application_id"],
        )

    async def _on_ComplianceClearanceIssued(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                state             = 'PENDING_DECISION',
                compliance_status = 'PASSED',
                last_event_type   = $1,
                last_event_at     = $2,
                data_as_of        = $2,
                updated_at        = NOW()
            WHERE application_id = $3
            """,
            event.event_type,
            event.recorded_at,
            event.payload["application_id"],
        )

    async def _on_DecisionGenerated(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        recommendation = p.get("recommendation", "")
        new_state = (
            "APPROVED_PENDING_HUMAN"
            if recommendation in ("APPROVE", "REFER")
            else "DECLINED_PENDING_HUMAN"
        )
        await conn.execute(
            """
            UPDATE application_summary SET
                state           = $1,
                decision        = $2,
                last_event_type = $3,
                last_event_at   = $4,
                data_as_of      = $4,
                updated_at      = NOW()
            WHERE application_id = $5
            """,
            new_state,
            recommendation,
            event.event_type,
            event.recorded_at,
            p["application_id"],
        )

    async def _on_HumanReviewCompleted(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                human_reviewer_id = $1,
                decision          = $2,
                last_event_type   = $3,
                last_event_at     = $4,
                data_as_of        = $4,
                updated_at        = NOW()
            WHERE application_id = $5
            """,
            event.payload["reviewer_id"],
            event.payload["final_decision"],
            event.event_type,
            event.recorded_at,
            event.payload["application_id"],
        )

    async def _on_ApplicationApproved(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                state             = 'FINAL_APPROVED',
                approved_amount_usd = $1,
                final_decision_at = $2,
                last_event_type   = $3,
                last_event_at     = $2,
                data_as_of        = $2,
                updated_at        = NOW()
            WHERE application_id = $4
            """,
            event.payload["approved_amount_usd"],
            event.recorded_at,
            event.event_type,
            event.payload["application_id"],
        )

    async def _on_ApplicationDeclined(
        self, event: "StoredEvent", conn: asyncpg.Connection
    ) -> None:
        await conn.execute(
            """
            UPDATE application_summary SET
                state             = 'FINAL_DECLINED',
                final_decision_at = $1,
                last_event_type   = $2,
                last_event_at     = $1,
                data_as_of        = $1,
                updated_at        = NOW()
            WHERE application_id = $3
            """,
            event.recorded_at,
            event.event_type,
            event.payload["application_id"],
        )
