"""
src/what_if/projector.py
=========================
What-If Counterfactual Projector

Answers: "What would the final decision have been if we had used
a different credit analysis result?"

How it works:
  1. Load all events for the application up to the branch point
  2. At the branch point, inject counterfactual events instead
  3. Continue replaying real events that are causally INDEPENDENT
  4. Skip real events causally DEPENDENT on the branched events
  5. Apply all events to projections in memory (never to real store)
  6. Return: real_outcome, counterfactual_outcome, divergence_events[]

CRITICAL: Counterfactual events are NEVER written to the real store.
This is enforced by using an in-memory event list, not the EventStore.

Causal dependency:
  An event is dependent if its causation_id traces back to an event
  at or after the branch point.

Video Demo — Step 6:
  Substitute HIGH risk tier for MEDIUM in credit analysis.
  Show cascading effect: confidence floor may force REFER,
  state machine transitions differ, final decision changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.event_store import EventStore
    from src.models.events import BaseEvent, StoredEvent

from src.models.events import (
    ApplicationState,
    Recommendation,
    StoredEvent,
)

logger = logging.getLogger(__name__)


@dataclass
class WhatIfResult:
    """Result of a what-if counterfactual analysis."""
    application_id:        str
    branch_at_event_type:  str
    branch_at_position:    int

    # Real outcome
    real_events:           list[dict]
    real_final_state:      str
    real_recommendation:   str | None
    real_confidence:       float | None

    # Counterfactual outcome
    counterfactual_events: list[dict]
    counterfactual_final_state: str
    counterfactual_recommendation: str | None
    counterfactual_confidence: float | None

    # Divergence analysis
    divergence_events:     list[str]   # event types that differed
    outcome_changed:       bool
    divergence_summary:    str


async def run_what_if(
    store: "EventStore",
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list["BaseEvent"],
    projections: list | None = None,
) -> WhatIfResult:
    """
    Run a counterfactual scenario by branching the event stream at a
    specific event type and injecting substitute events.

    Args:
        store:                EventStore (read-only in this function)
        application_id:       The application to analyse
        branch_at_event_type: Event type to branch at (e.g. "CreditAnalysisCompleted")
        counterfactual_events: Events to inject instead of real ones at branch point
        projections:          Optional projections to evaluate (default: loan state machine)

    Returns:
        WhatIfResult with real and counterfactual outcomes compared

    NEVER writes to the real store.
    """
    from src.models.events import StreamNotFoundError

    loan_stream = f"loan-{application_id}"
    compliance_stream = f"compliance-{application_id}"

    # ── Step 1: Load all real events ─────────────────────────────────────────
    try:
        real_events = await store.load_stream(loan_stream)
    except StreamNotFoundError:
        raise ValueError(f"Application stream not found: {loan_stream}")

    try:
        compliance_events = await store.load_stream(compliance_stream)
    except StreamNotFoundError:
        compliance_events = []

    # ── Step 2: Find the branch point ────────────────────────────────────────
    branch_position = None
    pre_branch_events = []
    post_branch_real_events = []

    for event in real_events:
        if event.event_type == branch_at_event_type and branch_position is None:
            branch_position = event.stream_position
            # The real branch event is excluded — replaced by counterfactual
        elif branch_position is None:
            pre_branch_events.append(event)
        else:
            post_branch_real_events.append(event)

    if branch_position is None:
        raise ValueError(
            f"Branch event type '{branch_at_event_type}' not found "
            f"in stream {loan_stream}"
        )

    logger.info(
        "What-if branch: app=%s, branch_at=%s (position %d), "
        "%d pre-branch events, %d post-branch events",
        application_id, branch_at_event_type, branch_position,
        len(pre_branch_events), len(post_branch_real_events),
    )

    # ── Step 3: Identify causally dependent post-branch events ────────────────
    # An event is causally dependent if its causation_id traces back
    # to an event at or after the branch point.
    # For simplicity: events that are direct consequences of the branched
    # event type are considered dependent.
    dependent_event_types = _get_dependent_event_types(branch_at_event_type)

    independent_post_branch = []
    skipped_dependent = []

    for event in post_branch_real_events:
        if event.event_type in dependent_event_types:
            skipped_dependent.append(event.event_type)
            logger.debug("Skipping dependent event: %s", event.event_type)
        else:
            independent_post_branch.append(event)

    # ── Step 4: Build counterfactual event sequence ────────────────────────────
    # pre_branch_events + counterfactual_events + independent_post_branch
    # Convert counterfactual BaseEvents to StoredEvent-like dicts for processing
    counterfactual_sequence = (
        pre_branch_events +
        _wrap_counterfactual_events(counterfactual_events, branch_position) +
        independent_post_branch
    )

    # ── Step 5: Evaluate both sequences through the loan state machine ─────────
    real_sequence = pre_branch_events + post_branch_real_events

    real_state = _evaluate_sequence(real_sequence)
    cf_state = _evaluate_sequence(counterfactual_sequence)

    # ── Step 6: Compare outcomes ──────────────────────────────────────────────
    divergence_events = []
    if real_state["final_state"] != cf_state["final_state"]:
        divergence_events.append(f"final_state: {real_state['final_state']} → {cf_state['final_state']}")
    if real_state["recommendation"] != cf_state["recommendation"]:
        divergence_events.append(
            f"recommendation: {real_state['recommendation']} → {cf_state['recommendation']}"
        )
    if real_state["risk_tier"] != cf_state["risk_tier"]:
        divergence_events.append(
            f"risk_tier: {real_state['risk_tier']} → {cf_state['risk_tier']}"
        )

    outcome_changed = bool(divergence_events)

    divergence_summary = (
        f"Counterfactual produced a DIFFERENT outcome: {'; '.join(divergence_events)}"
        if outcome_changed
        else "Counterfactual produced the SAME outcome as the real scenario."
    )

    logger.info(
        "What-if result: outcome_changed=%s, real=%s, counterfactual=%s",
        outcome_changed,
        real_state["final_state"],
        cf_state["final_state"],
    )

    return WhatIfResult(
        application_id=application_id,
        branch_at_event_type=branch_at_event_type,
        branch_at_position=branch_position,
        real_events=_serialise_events(real_sequence),
        real_final_state=real_state["final_state"],
        real_recommendation=real_state["recommendation"],
        real_confidence=real_state["confidence"],
        counterfactual_events=_serialise_events(counterfactual_sequence),
        counterfactual_final_state=cf_state["final_state"],
        counterfactual_recommendation=cf_state["recommendation"],
        counterfactual_confidence=cf_state["confidence"],
        divergence_events=divergence_events,
        outcome_changed=outcome_changed,
        divergence_summary=divergence_summary,
    )


def _get_dependent_event_types(branch_event_type: str) -> set[str]:
    """
    Return event types that are causally dependent on the branch event.
    These are skipped in the counterfactual sequence because they were
    consequences of the real branch event, not the counterfactual one.
    """
    dependency_map = {
        "CreditAnalysisCompleted": {
            # The decision and approval depend on the credit analysis result
            "DecisionGenerated",
            "HumanReviewCompleted",
            "ApplicationApproved",
            "ApplicationDeclined",
            "ApplicationUnderReview",
        },
        "FraudScreeningCompleted": {
            "DecisionGenerated",
            "HumanReviewCompleted",
            "ApplicationApproved",
            "ApplicationDeclined",
        },
        "DecisionGenerated": {
            "HumanReviewCompleted",
            "ApplicationApproved",
            "ApplicationDeclined",
        },
    }
    return dependency_map.get(branch_event_type, set())


def _wrap_counterfactual_events(
    events: list["BaseEvent"],
    start_position: int,
) -> list[StoredEvent]:
    """
    Wrap BaseEvent counterfactual events in StoredEvent shell for processing.
    These are in-memory only — never persisted.
    """
    from uuid import uuid4
    from datetime import datetime, timezone

    wrapped = []
    for i, event in enumerate(events):
        wrapped.append(StoredEvent(
            event_id=uuid4(),
            stream_id="COUNTERFACTUAL",
            stream_position=start_position + i,
            global_position=999000 + i,  # high position to avoid collision
            event_type=event.event_type,
            event_version=event.event_version,
            payload=event.get_payload(),
            metadata={"counterfactual": True},
            recorded_at=datetime.now(timezone.utc),
        ))
    return wrapped


def _evaluate_sequence(events: list[StoredEvent]) -> dict[str, Any]:
    """
    Evaluate a sequence of events through the loan application state machine.
    Returns the final state, recommendation, risk tier, and confidence.
    Pure function — no side effects.
    """
    state = ApplicationState.SUBMITTED.value
    recommendation = None
    risk_tier = None
    confidence = None
    approved_amount = None

    STATE_TRANSITIONS = {
        "CreditAnalysisRequested":   "AWAITING_ANALYSIS",
        "CreditAnalysisCompleted":   "ANALYSIS_COMPLETE",
        "ComplianceCheckRequested":  "COMPLIANCE_REVIEW",
        "ComplianceClearanceIssued": "PENDING_DECISION",
        "ApplicationApproved":       "FINAL_APPROVED",
        "ApplicationDeclined":       "FINAL_DECLINED",
    }

    for event in events:
        et = event.event_type
        p = event.payload

        if et in STATE_TRANSITIONS:
            state = STATE_TRANSITIONS[et]

        if et == "CreditAnalysisCompleted":
            risk_tier = p.get("risk_tier")
            confidence = p.get("confidence_score")

        if et == "DecisionGenerated":
            recommendation = p.get("recommendation")
            confidence = p.get("confidence_score")
            # Enforce confidence floor
            if confidence is not None and confidence < 0.6:
                recommendation = "REFER"
            # Determine pending state
            if recommendation in ("APPROVE", "REFER"):
                state = "APPROVED_PENDING_HUMAN"
            else:
                state = "DECLINED_PENDING_HUMAN"

        if et == "ApplicationApproved":
            state = "FINAL_APPROVED"
            approved_amount = p.get("approved_amount_usd")

        if et == "ApplicationDeclined":
            state = "FINAL_DECLINED"

    return {
        "final_state": state,
        "recommendation": recommendation,
        "risk_tier": risk_tier,
        "confidence": confidence,
        "approved_amount": approved_amount,
    }


def _serialise_events(events: list[StoredEvent]) -> list[dict]:
    """Serialise events to dicts for the WhatIfResult."""
    result = []
    for e in events:
        result.append({
            "event_type": e.event_type,
            "stream_position": e.stream_position,
            "payload": e.payload,
            "counterfactual": e.metadata.get("counterfactual", False),
        })
    return result
