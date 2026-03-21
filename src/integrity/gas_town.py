"""
src/integrity/gas_town.py
==========================
Gas Town Agent Memory Pattern — crash recovery from event stream.

Named for the infrastructure pattern where agent context is lost on
process restart. The solution: every agent action was written to the
event store before execution. On restart, the agent replays its
AgentSession stream to reconstruct its context window and continue
exactly where it left off.

Used in Step 5 of video demo:
  1. Start agent session, append several events
  2. Simulate crash (del agent object)
  3. Call reconstruct_agent_context()
  4. Show agent can resume with correct state
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.event_store import EventStore

from src.models.events import SessionHealthStatus, StoredEvent

logger = logging.getLogger(__name__)

# Events that indicate completed work (safe to summarise)
COMPLETED_EVENT_TYPES = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceClearanceIssued",
    "DecisionGenerated",
    "AgentSessionClosed",
}

# Events that indicate PENDING/incomplete work (must preserve verbatim)
PENDING_EVENT_TYPES = {
    "CreditAnalysisRequested",
    "ComplianceCheckRequested",
}

# Events that always must be preserved verbatim (last N + these)
ALWAYS_PRESERVE_TYPES = {
    "AgentContextLoaded",  # context source and model version
}


@dataclass
class AgentContext:
    """
    Reconstructed agent context from event stream replay.
    Contains everything the agent needs to resume work.
    """
    agent_id:              str
    session_id:            str
    model_version:         str
    context_source:        str

    # Reconstructed working memory
    context_text:          str
    last_event_position:   int

    # Work state
    applications_processed: list[str]
    pending_work:          list[dict[str, Any]]
    completed_work:        list[dict[str, Any]]

    # Health
    session_health_status: SessionHealthStatus
    needs_reconciliation:  bool
    reconciliation_reason: str | None

    # Metadata
    total_events_replayed: int
    context_token_estimate: int
    reconstructed_at:      datetime


async def reconstruct_agent_context(
    store: "EventStore",
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Reconstruct agent context from the event store after a crash.

    Steps:
    1. Load full AgentSession stream
    2. Identify last completed action, pending work, current application state
    3. Summarise old events into prose (token-efficient)
    4. Preserve verbatim: last 3 events + any PENDING/ERROR state events
    5. Detect NEEDS_RECONCILIATION: last event was a partial decision

    CRITICAL: If the agent's last event was a partial action (no
    corresponding completion event), flag NEEDS_RECONCILIATION.
    The agent must resolve this before proceeding.

    Args:
        store:        EventStore instance
        agent_id:     The agent's identifier
        session_id:   The session to reconstruct
        token_budget: Maximum tokens for context_text (default 8000)

    Returns:
        AgentContext with full reconstruction details
    """
    from src.models.events import StreamNotFoundError
    from datetime import timezone
    import datetime as dt

    stream_id = f"agent-{agent_id}-{session_id}"

    # ── Step 1: Load full event stream ───────────────────────────────────────
    try:
        events = await store.load_stream(stream_id)
    except StreamNotFoundError:
        raise ValueError(
            f"AgentSession stream not found: {stream_id}. "
            "The agent may have never started or used a different session_id."
        )

    if not events:
        raise ValueError(f"AgentSession stream {stream_id} is empty.")

    total_events = len(events)

    # ── Step 2: Classify events ───────────────────────────────────────────────
    context_loaded_event: StoredEvent | None = None
    pending_work: list[dict] = []
    completed_work: list[dict] = []
    applications_processed: list[str] = []
    model_version = "unknown"
    context_source = "unknown"

    for event in events:
        if event.event_type == "AgentContextLoaded":
            context_loaded_event = event
            model_version = event.payload.get("model_version", "unknown")
            context_source = event.payload.get("context_source", "unknown")

        elif event.event_type in COMPLETED_EVENT_TYPES:
            app_id = event.payload.get("application_id")
            if app_id and app_id not in applications_processed:
                applications_processed.append(app_id)
            completed_work.append({
                "event_type": event.event_type,
                "position":   event.stream_position,
                "payload":    event.payload,
                "recorded_at": str(event.recorded_at),
            })

        elif event.event_type in PENDING_EVENT_TYPES:
            app_id = event.payload.get("application_id")
            pending_work.append({
                "event_type": event.event_type,
                "position":   event.stream_position,
                "payload":    event.payload,
                "recorded_at": str(event.recorded_at),
            })

    # Remove pending items that were subsequently completed
    completed_app_ids = {
        w["payload"].get("application_id")
        for w in completed_work
    }
    pending_work = [
        w for w in pending_work
        if w["payload"].get("application_id") not in completed_app_ids
    ]

    # ── Step 3: Detect NEEDS_RECONCILIATION ──────────────────────────────────
    last_event = events[-1]
    needs_reconciliation = False
    reconciliation_reason = None

    if last_event.event_type in PENDING_EVENT_TYPES:
        needs_reconciliation = True
        reconciliation_reason = (
            f"Session ended with a pending {last_event.event_type} "
            f"at position {last_event.stream_position} for application "
            f"{last_event.payload.get('application_id', 'unknown')}. "
            "The agent must check whether this action was completed by "
            "another process before proceeding."
        )
    elif not context_loaded_event:
        needs_reconciliation = True
        reconciliation_reason = (
            "No AgentContextLoaded event found in session. "
            "Session may be corrupted — Gas Town context requirement violated."
        )

    health_status = (
        SessionHealthStatus.NEEDS_RECONCILIATION
        if needs_reconciliation
        else SessionHealthStatus.HEALTHY
    )

    # ── Step 4: Build context_text (token-efficient) ──────────────────────────
    # Strategy:
    #   - Always include context header (model, source, session info)
    #   - Summarise completed work in prose (1 sentence each)
    #   - Preserve verbatim: last 3 events + all pending events
    #   - Stay within token_budget

    preserve_verbatim = set()

    # Always preserve last 3 events
    for e in events[-3:]:
        preserve_verbatim.add(e.stream_position)

    # Always preserve pending events
    for w in pending_work:
        preserve_verbatim.add(w["position"])

    # Always preserve AgentContextLoaded
    if context_loaded_event:
        preserve_verbatim.add(context_loaded_event.stream_position)

    context_lines = []

    # Header
    context_lines.append(
        f"=== Agent Session Reconstruction ==="
    )
    context_lines.append(
        f"Agent: {agent_id} | Session: {session_id}"
    )
    context_lines.append(
        f"Model: {model_version} | Context source: {context_source}"
    )
    context_lines.append(
        f"Total events replayed: {total_events}"
    )
    if needs_reconciliation:
        context_lines.append(
            f"⚠ NEEDS RECONCILIATION: {reconciliation_reason}"
        )
    context_lines.append("")

    # Summary of completed work
    if completed_work:
        context_lines.append("--- Completed work (summary) ---")
        for work in completed_work:
            app_id = work["payload"].get("application_id", "unknown")
            et = work["event_type"]
            pos = work["position"]
            context_lines.append(
                f"  [{pos}] {et} for application {app_id}"
            )
        context_lines.append("")

    # Pending work (verbatim)
    if pending_work:
        context_lines.append("--- PENDING work (requires action) ---")
        for work in pending_work:
            context_lines.append(
                f"  [{work['position']}] {work['event_type']} — "
                f"application {work['payload'].get('application_id', 'unknown')}"
            )
        context_lines.append("")

    # Last 3 events verbatim
    context_lines.append("--- Last 3 events (verbatim) ---")
    for event in events[-3:]:
        context_lines.append(
            f"  [{event.stream_position}] {event.event_type}: "
            f"{_summarise_payload(event.payload)}"
        )

    context_text = "\n".join(context_lines)

    # Rough token estimate (1 token ≈ 4 chars)
    token_estimate = len(context_text) // 4

    # Truncate if over budget
    if token_estimate > token_budget:
        chars_allowed = token_budget * 4
        context_text = context_text[:chars_allowed] + "\n[... truncated to token budget]"
        token_estimate = token_budget

    logger.info(
        "Agent context reconstructed: %s/%s — %d events, health=%s, tokens≈%d",
        agent_id, session_id, total_events,
        health_status.value, token_estimate,
    )

    return AgentContext(
        agent_id=agent_id,
        session_id=session_id,
        model_version=model_version,
        context_source=context_source,
        context_text=context_text,
        last_event_position=last_event.stream_position,
        applications_processed=applications_processed,
        pending_work=pending_work,
        completed_work=completed_work,
        session_health_status=health_status,
        needs_reconciliation=needs_reconciliation,
        reconciliation_reason=reconciliation_reason,
        total_events_replayed=total_events,
        context_token_estimate=token_estimate,
        reconstructed_at=datetime.now(timezone.utc),
    )


def _summarise_payload(payload: dict) -> str:
    """Return a short human-readable summary of an event payload."""
    key_fields = ["application_id", "agent_id", "risk_tier",
                  "fraud_score", "recommendation", "confidence_score",
                  "rule_id", "status", "reason"]
    parts = []
    for field in key_fields:
        if field in payload and payload[field] is not None:
            parts.append(f"{field}={payload[field]}")
    return ", ".join(parts[:4]) if parts else str(payload)[:80]
