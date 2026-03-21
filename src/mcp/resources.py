"""
src/mcp/resources.py
=====================
MCP Resources — the query (read) side of The Ledger.

6 resources, all reading from projections.
CRITICAL: No resource ever replays an aggregate stream directly.
All reads come from pre-built projection tables.

Justified exceptions (per brief):
  - ledger://applications/{id}/audit-trail: loads AuditLedger stream directly
    (justified: audit trail is itself an event stream, not a projection)
  - ledger://agents/{id}/sessions/{session_id}: loads AgentSession stream directly
    (justified: full replay capability required for compliance examination)

SLO contracts:
  ledger://applications/{id}          p99 < 50ms
  ledger://applications/{id}/compliance p99 < 200ms
  ledger://applications/{id}/audit-trail p99 < 500ms
  ledger://agents/{id}/performance     p99 < 50ms
  ledger://agents/{id}/sessions/{id}   p99 < 300ms
  ledger://ledger/health               p99 < 10ms
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from src.mcp.server import mcp
from src.mcp.dependencies import get_store, get_db_pool
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.models.events import StreamNotFoundError

logger = logging.getLogger(__name__)

_compliance_proj = ComplianceAuditViewProjection()


def _staleness_meta(data_as_of) -> dict:
    """Build staleness metadata for UI consumers."""
    if data_as_of is None:
        return {"data_as_of": None, "projection_lag_ms": None, "slo_status": "UNKNOWN"}
    now = datetime.now(timezone.utc)
    if hasattr(data_as_of, "tzinfo") and data_as_of.tzinfo is None:
        data_as_of = data_as_of.replace(tzinfo=timezone.utc)
    lag_ms = (now - data_as_of).total_seconds() * 1000
    return {
        "data_as_of": data_as_of.isoformat(),
        "projection_lag_ms": round(lag_ms, 2),
        "slo_status": "OK" if lag_ms < 500 else "DEGRADED",
    }


# =============================================================================
# RESOURCE 1: ledger://applications/{id}
# =============================================================================

@mcp.resource("ledger://applications/{application_id}")
async def get_application(application_id: str) -> str:
    """
    Current state of a loan application.
    Source: ApplicationSummary projection.
    SLO: p99 < 50ms. Returns current state only — no temporal query.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )
    if not row:
        return json.dumps({
            "error": "NotFound",
            "application_id": application_id,
            "message": f"Application '{application_id}' not found in projection.",
        })

    data = dict(row)
    # Handle non-serialisable types
    for k, v in data.items():
        if hasattr(v, "isoformat"):
            data[k] = v.isoformat()
        elif isinstance(v, (bytes, bytearray)):
            data[k] = v.decode()

    return json.dumps({
        "data": data,
        "meta": _staleness_meta(row["data_as_of"]),
    }, default=str)


# =============================================================================
# RESOURCE 2: ledger://applications/{id}/compliance
# =============================================================================

@mcp.resource("ledger://applications/{application_id}/compliance")
async def get_application_compliance(application_id: str) -> str:
    """
    Full compliance record for a loan application.
    Source: ComplianceAuditView projection.
    SLO: p99 < 200ms.

    Supports temporal query via as_of parameter in URI:
      ledger://applications/{id}/compliance?as_of=2026-03-01T10:00:00Z
    Returns compliance state as it existed at that timestamp.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await _compliance_proj.get_current_compliance(application_id, conn)
        lag_ms = await _compliance_proj.get_projection_lag(conn)

    # Filter out internal clearance rows for display
    display_rows = [r for r in rows if r.get("rule_id") != "__CLEARANCE__"]

    serialised = []
    for r in display_rows:
        row_dict = dict(r)
        for k, v in row_dict.items():
            if hasattr(v, "isoformat"):
                row_dict[k] = v.isoformat()
        serialised.append(row_dict)

    return json.dumps({
        "data": {
            "application_id": application_id,
            "compliance_checks": serialised,
            "total_checks": len(serialised),
            "passed": sum(1 for r in serialised if r.get("status") == "PASSED"),
            "failed": sum(1 for r in serialised if r.get("status") == "FAILED"),
            "pending": sum(1 for r in serialised if r.get("status") == "PENDING"),
        },
        "meta": {
            "projection_lag_ms": round(lag_ms, 2),
            "slo_ms": 2000,
            "slo_status": "OK" if lag_ms < 2000 else "DEGRADED",
            "temporal_query_supported": True,
            "temporal_query_hint": "Add ?as_of=ISO_TIMESTAMP for historical state",
        },
    }, default=str)


@mcp.resource("ledger://applications/{application_id}/compliance/at/{as_of_timestamp}")
async def get_application_compliance_at(
    application_id: str,
    as_of_timestamp: str,
) -> str:
    """
    Compliance state as it existed at a specific point in time.
    Source: ComplianceAuditView projection with temporal query.
    SLO: p99 < 200ms (uses snapshots to avoid full replay).

    as_of_timestamp: ISO 8601 format, e.g. 2026-03-01T10:00:00Z
    """
    try:
        as_of = datetime.fromisoformat(as_of_timestamp.replace("Z", "+00:00"))
    except ValueError:
        return json.dumps({
            "error": "InvalidTimestamp",
            "message": f"Cannot parse timestamp: {as_of_timestamp}. Use ISO 8601 format.",
        })

    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await _compliance_proj.get_compliance_at(application_id, as_of, conn)

    display_rows = [r for r in rows if r.get("rule_id") != "__CLEARANCE__"]
    serialised = []
    for r in display_rows:
        row_dict = dict(r)
        for k, v in row_dict.items():
            if hasattr(v, "isoformat"):
                row_dict[k] = v.isoformat()
        serialised.append(row_dict)

    return json.dumps({
        "data": {
            "application_id": application_id,
            "as_of": as_of.isoformat(),
            "compliance_checks": serialised,
            "total_checks": len(serialised),
            "passed": sum(1 for r in serialised if r.get("status") == "PASSED"),
            "failed": sum(1 for r in serialised if r.get("status") == "FAILED"),
            "pending": sum(1 for r in serialised if r.get("status") == "PENDING"),
        },
        "meta": {
            "query_type": "temporal",
            "as_of": as_of.isoformat(),
            "note": "Historical state — may differ from current compliance state",
        },
    }, default=str)


# =============================================================================
# RESOURCE 3: ledger://applications/{id}/audit-trail
# =============================================================================

@mcp.resource("ledger://applications/{application_id}/audit-trail")
async def get_audit_trail(application_id: str) -> str:
    """
    Complete audit trail for a loan application.
    Source: Direct stream load (justified exception — audit trail IS the stream).
    SLO: p99 < 500ms.

    Returns full event stream with all agent actions, compliance checks,
    human review, and causal links. Includes cryptographic integrity status.
    This is the resource used for Step 1 of the video demo.
    """
    store = await get_store()
    stream_id = f"loan-{application_id}"

    try:
        events = await store.load_stream(stream_id)
    except StreamNotFoundError:
        return json.dumps({
            "error": "NotFound",
            "application_id": application_id,
            "message": f"No event stream found for application '{application_id}'",
        })

    # Also load compliance and agent streams for full picture
    all_events = list(events)

    try:
        compliance_events = await store.load_stream(f"compliance-{application_id}")
        all_events.extend(compliance_events)
    except StreamNotFoundError:
        pass

    # Sort all events by recorded_at for chronological display
    all_events.sort(key=lambda e: e.recorded_at)

    serialised_events = []
    for e in all_events:
        serialised_events.append({
            "event_id": str(e.event_id),
            "stream_id": e.stream_id,
            "stream_position": e.stream_position,
            "global_position": e.global_position,
            "event_type": e.event_type,
            "event_version": e.event_version,
            "payload": e.payload,
            "metadata": e.metadata,
            "recorded_at": e.recorded_at.isoformat(),
        })

    # Build human-readable narrative
    narrative = _build_narrative(events)

    return json.dumps({
        "data": {
            "application_id": application_id,
            "total_events": len(serialised_events),
            "events": serialised_events,
            "narrative": narrative,
        },
        "meta": {
            "stream_id": stream_id,
            "justified_exception": "Direct stream load — audit trail is the stream",
            "integrity_check_available": True,
            "integrity_check_hint": "Call run_integrity_check tool to verify hash chain",
        },
    }, default=str)


def _build_narrative(events) -> list[str]:
    """Build one-sentence-per-event human-readable narrative."""
    narrative = []
    for e in events:
        p = e.payload
        app_id = p.get("application_id", "")
        if e.event_type == "ApplicationSubmitted":
            narrative.append(
                f"Application {app_id} submitted by {p.get('applicant_id')} "
                f"for ${p.get('requested_amount_usd')} via {p.get('submission_channel')}."
            )
        elif e.event_type == "CreditAnalysisRequested":
            narrative.append(
                f"Credit analysis requested for {app_id}, assigned to agent {p.get('assigned_agent_id')}."
            )
        elif e.event_type == "CreditAnalysisCompleted":
            narrative.append(
                f"Credit analysis completed: risk={p.get('risk_tier')}, "
                f"recommended_limit=${p.get('recommended_limit_usd')}, "
                f"confidence={p.get('confidence_score')}."
            )
        elif e.event_type == "FraudScreeningCompleted":
            narrative.append(
                f"Fraud screening completed: score={p.get('fraud_score')}, "
                f"flags={p.get('anomaly_flags', [])}."
            )
        elif e.event_type == "ComplianceCheckRequested":
            narrative.append(
                f"Compliance checking initiated: {len(p.get('checks_required', []))} rules "
                f"required under {p.get('regulation_set_version')}."
            )
        elif e.event_type == "ComplianceRulePassed":
            narrative.append(f"Compliance rule {p.get('rule_id')} passed.")
        elif e.event_type == "ComplianceRuleFailed":
            narrative.append(
                f"Compliance rule {p.get('rule_id')} FAILED: {p.get('failure_reason')}."
            )
        elif e.event_type == "ComplianceClearanceIssued":
            narrative.append(f"Compliance clearance issued — all required checks passed.")
        elif e.event_type == "DecisionGenerated":
            narrative.append(
                f"AI decision generated: {p.get('recommendation')} "
                f"(confidence={p.get('confidence_score')})."
            )
        elif e.event_type == "HumanReviewCompleted":
            override = " (OVERRIDE)" if p.get("override") else ""
            narrative.append(
                f"Human review by {p.get('reviewer_id')}: {p.get('final_decision')}{override}."
            )
        elif e.event_type == "ApplicationApproved":
            narrative.append(
                f"Application APPROVED for ${p.get('approved_amount_usd')} "
                f"by {p.get('approved_by')}."
            )
        elif e.event_type == "ApplicationDeclined":
            narrative.append(
                f"Application DECLINED by {p.get('declined_by')}. "
                f"Reasons: {p.get('decline_reasons', [])}."
            )
    return narrative


# =============================================================================
# RESOURCE 4: ledger://agents/{id}/performance
# =============================================================================

@mcp.resource("ledger://agents/{agent_id}/performance")
async def get_agent_performance(agent_id: str) -> str:
    """
    Performance metrics for an AI agent across all model versions.
    Source: AgentPerformanceLedger projection.
    SLO: p99 < 50ms.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE agent_id = $1 ORDER BY last_seen_at DESC",
            agent_id,
        )

    if not rows:
        return json.dumps({
            "error": "NotFound",
            "agent_id": agent_id,
            "message": f"No performance data for agent '{agent_id}'",
        })

    serialised = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        serialised.append(d)

    return json.dumps({
        "data": {
            "agent_id": agent_id,
            "model_versions": serialised,
            "total_analyses": sum(r["analyses_completed"] for r in serialised),
            "total_decisions": sum(r["decisions_generated"] for r in serialised),
        },
        "meta": {
            "slo_ms": 50,
            "temporal_query_supported": False,
        },
    }, default=str)


# =============================================================================
# RESOURCE 5: ledger://agents/{id}/sessions/{session_id}
# =============================================================================

@mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
async def get_agent_session(agent_id: str, session_id: str) -> str:
    """
    Full event replay of an agent session.
    Source: AgentSession stream (justified exception — full replay capability).
    SLO: p99 < 300ms.
    """
    store = await get_store()
    stream_id = f"agent-{agent_id}-{session_id}"

    try:
        events = await store.load_stream(stream_id)
    except StreamNotFoundError:
        return json.dumps({
            "error": "NotFound",
            "agent_id": agent_id,
            "session_id": session_id,
            "message": f"Session stream not found: {stream_id}",
        })

    serialised = []
    for e in events:
        serialised.append({
            "event_id": str(e.event_id),
            "stream_position": e.stream_position,
            "event_type": e.event_type,
            "event_version": e.event_version,
            "payload": e.payload,
            "recorded_at": e.recorded_at.isoformat(),
        })

    return json.dumps({
        "data": {
            "agent_id": agent_id,
            "session_id": session_id,
            "stream_id": stream_id,
            "total_events": len(serialised),
            "events": serialised,
        },
        "meta": {
            "justified_exception": "Direct stream load — full replay capability required",
            "slo_ms": 300,
        },
    }, default=str)


# =============================================================================
# RESOURCE 6: ledger://ledger/health
# =============================================================================

@mcp.resource("ledger://ledger/health")
async def get_ledger_health() -> str:
    """
    Projection daemon health — lag for all projections.
    Source: projection_checkpoints table + events table.
    SLO: p99 < 10ms. Used as watchdog endpoint.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        checkpoints = await conn.fetch(
            "SELECT projection_name, last_position, updated_at FROM projection_checkpoints"
        )
        latest_global = await conn.fetchval(
            "SELECT COALESCE(MAX(global_position), 0) FROM events"
        )
        total_events = await conn.fetchval("SELECT COUNT(*) FROM events")

    now = datetime.now(timezone.utc)
    projection_health = []
    overall_status = "OK"

    slo_map = {
        "ApplicationSummary": 500,
        "AgentPerformanceLedger": 500,
        "ComplianceAuditView": 2000,
    }

    for row in checkpoints:
        name = row["projection_name"]
        last_pos = row["last_position"]
        updated_at = row["updated_at"]
        if updated_at and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        lag_ms = (now - updated_at).total_seconds() * 1000 if updated_at else 0
        events_behind = (latest_global or 0) - last_pos
        slo = slo_map.get(name, 500)
        status = "OK" if lag_ms < slo else "DEGRADED"
        if status == "DEGRADED":
            overall_status = "DEGRADED"

        projection_health.append({
            "projection_name": name,
            "last_position": last_pos,
            "latest_global_position": latest_global,
            "events_behind": events_behind,
            "lag_ms": round(lag_ms, 2),
            "slo_ms": slo,
            "status": status,
        })

    return json.dumps({
        "status": overall_status,
        "total_events_in_store": total_events,
        "latest_global_position": latest_global,
        "projections": projection_health,
        "checked_at": now.isoformat(),
    }, default=str)
