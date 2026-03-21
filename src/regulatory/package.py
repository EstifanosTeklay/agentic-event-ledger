"""
src/regulatory/package.py
==========================
Regulatory Examination Package Generator

Produces a complete, self-contained JSON examination package that a
regulator can verify against the database independently — they should
not need to trust the system to validate that the package is accurate.

Package contains:
  1. Complete event stream in order with full payloads
  2. State of every projection at examination_date
  3. Audit chain integrity verification result
  4. Human-readable narrative of the application lifecycle
  5. Model versions, confidence scores, and input data hashes
     for every AI agent that participated in the decision

Used in Phase 6 (bonus) and referenced in DESIGN.md.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.event_store import EventStore

from src.integrity.audit_chain import run_integrity_check, verify_chain_integrity

logger = logging.getLogger(__name__)


async def generate_regulatory_package(
    store: "EventStore",
    application_id: str,
    examination_date: datetime | None = None,
    pool=None,
) -> dict[str, Any]:
    """
    Generate a complete regulatory examination package.

    The package is self-contained and independently verifiable.
    A regulator can recompute the integrity hash from the event payloads
    and verify it matches the recorded hash — without trusting the system.

    Args:
        store:            EventStore instance
        application_id:   The application to examine
        examination_date: Point-in-time for projection snapshots
                         (default: now)
        pool:            asyncpg pool for projection queries

    Returns:
        dict — complete examination package (JSON-serialisable)
    """
    from src.models.events import StreamNotFoundError
    from src.projections.compliance_audit import ComplianceAuditViewProjection

    if examination_date is None:
        examination_date = datetime.now(timezone.utc)

    logger.info(
        "Generating regulatory package: app=%s, examination_date=%s",
        application_id, examination_date.isoformat(),
    )

    loan_stream      = f"loan-{application_id}"
    compliance_stream = f"compliance-{application_id}"
    package_generated_at = datetime.now(timezone.utc)

    # ── Section 1: Complete event stream ─────────────────────────────────────
    try:
        loan_events = await store.load_stream(loan_stream)
    except StreamNotFoundError:
        raise ValueError(f"Application not found: {application_id}")

    try:
        compliance_events = await store.load_stream(compliance_stream)
    except StreamNotFoundError:
        compliance_events = []

    all_events = sorted(
        list(loan_events) + list(compliance_events),
        key=lambda e: e.recorded_at,
    )

    serialised_events = []
    for e in all_events:
        serialised_events.append({
            "event_id":        str(e.event_id),
            "stream_id":       e.stream_id,
            "stream_position": e.stream_position,
            "global_position": e.global_position,
            "event_type":      e.event_type,
            "event_version":   e.event_version,
            "payload":         e.payload,
            "metadata":        e.metadata,
            "recorded_at":     e.recorded_at.isoformat(),
            "payload_hash":    _hash_payload(e.payload),
        })

    # ── Section 2: Projection states at examination_date ─────────────────────
    projection_states: dict[str, Any] = {}

    if pool:
        # ApplicationSummary at examination_date
        async with pool.acquire() as conn:
            app_row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1",
                application_id,
            )
        if app_row:
            app_dict = dict(app_row)
            for k, v in app_dict.items():
                if hasattr(v, "isoformat"):
                    app_dict[k] = v.isoformat()
            projection_states["ApplicationSummary"] = app_dict

        # ComplianceAuditView at examination_date
        compliance_proj = ComplianceAuditViewProjection()
        async with pool.acquire() as conn:
            compliance_at = await compliance_proj.get_compliance_at(
                application_id, examination_date, conn
            )
        serialised_compliance = []
        for row in compliance_at:
            r = dict(row)
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
            serialised_compliance.append(r)
        projection_states["ComplianceAuditView"] = {
            "as_of": examination_date.isoformat(),
            "checks": serialised_compliance,
        }

    # ── Section 3: Integrity verification ────────────────────────────────────
    integrity_result = await run_integrity_check(
        store=store,
        entity_type="LoanApplication",
        entity_id=application_id,
    )

    integrity_section = {
        "events_verified":  integrity_result.events_verified,
        "chain_valid":      integrity_result.chain_valid,
        "tamper_detected":  integrity_result.tamper_detected,
        "integrity_hash":   integrity_result.integrity_hash,
        "previous_hash":    integrity_result.previous_hash,
        "check_timestamp":  integrity_result.check_timestamp.isoformat(),
        "verification_method": (
            "SHA-256 hash chain: each check hashes all event payloads "
            "since the previous check, combined with the previous integrity hash. "
            "Recompute: sha256(previous_hash + sha256(payload_1) + sha256(payload_2) + ...)"
        ),
        "independent_verification": (
            "To verify independently: compute sha256 of each event's payload "
            "(sorted JSON, keys alphabetical), concatenate with previous_hash, "
            "compute sha256 of the result. Compare with integrity_hash above."
        ),
    }

    # ── Section 4: Human-readable narrative ──────────────────────────────────
    narrative = _build_full_narrative(loan_events, compliance_events)

    # ── Section 5: AI agent metadata ─────────────────────────────────────────
    agent_metadata = _extract_agent_metadata(loan_events, store)

    # ── Assemble package ──────────────────────────────────────────────────────
    package = {
        "package_type":     "RegulatoryExaminationPackage",
        "schema_version":   "1.0",
        "application_id":   application_id,
        "examination_date": examination_date.isoformat(),
        "generated_at":     package_generated_at.isoformat(),
        "generated_by":     "The Ledger — Agentic Event Store",

        "section_1_event_stream": {
            "description": (
                "Complete immutable event stream for this application. "
                "Events are in chronological order. payload_hash enables "
                "independent verification of each event's integrity."
            ),
            "total_events": len(serialised_events),
            "events": serialised_events,
        },

        "section_2_projection_states": {
            "description": (
                "State of read-model projections as they existed at examination_date. "
                "Derived from section_1_event_stream by replaying events up to "
                "examination_date."
            ),
            "projections": projection_states,
        },

        "section_3_integrity": {
            "description": (
                "Cryptographic audit chain verification. Chain is built by "
                "hashing all event payloads in sequence. Any modification to "
                "any event payload breaks the chain."
            ),
            **integrity_section,
        },

        "section_4_narrative": {
            "description": "Human-readable lifecycle summary. One entry per significant event.",
            "entries": narrative,
        },

        "section_5_ai_agent_metadata": {
            "description": (
                "Model versions, confidence scores, and input data hashes "
                "for every AI agent that participated in this decision."
            ),
            "agents": agent_metadata,
        },

        "package_hash": None,  # computed below
    }

    # Compute package hash for self-integrity
    package_str = json.dumps(
        {k: v for k, v in package.items() if k != "package_hash"},
        sort_keys=True,
        default=str,
    )
    package["package_hash"] = hashlib.sha256(package_str.encode()).hexdigest()

    logger.info(
        "Regulatory package generated: app=%s, events=%d, chain_valid=%s",
        application_id, len(serialised_events), integrity_result.chain_valid,
    )

    return package


def _hash_payload(payload: dict) -> str:
    """SHA-256 hash of a payload dict (sorted keys for determinism)."""
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _build_full_narrative(loan_events, compliance_events) -> list[dict]:
    """Build detailed narrative entries with timestamps."""
    all_events = sorted(
        list(loan_events) + list(compliance_events),
        key=lambda e: e.recorded_at,
    )

    narrative = []
    for e in all_events:
        p = e.payload
        entry = {
            "timestamp":  e.recorded_at.isoformat(),
            "event_type": e.event_type,
            "stream":     e.stream_id,
            "summary":    _summarise_event(e.event_type, p),
        }
        narrative.append(entry)
    return narrative


def _summarise_event(event_type: str, p: dict) -> str:
    """One sentence per event type."""
    summaries = {
        "ApplicationSubmitted": (
            f"Application submitted by {p.get('applicant_id')} "
            f"for ${p.get('requested_amount_usd')} via {p.get('submission_channel')}."
        ),
        "CreditAnalysisRequested": (
            f"Credit analysis assigned to agent {p.get('assigned_agent_id')} "
            f"with priority {p.get('priority', 'NORMAL')}."
        ),
        "CreditAnalysisCompleted": (
            f"Credit analysis completed by agent {p.get('agent_id')} "
            f"using model {p.get('model_version', 'unknown')}: "
            f"risk={p.get('risk_tier')}, "
            f"recommended_limit=${p.get('recommended_limit_usd')}, "
            f"confidence={p.get('confidence_score')}, "
            f"input_hash={str(p.get('input_data_hash',''))[:16]}..."
        ),
        "FraudScreeningCompleted": (
            f"Fraud screening by agent {p.get('agent_id')}: "
            f"score={p.get('fraud_score')}, "
            f"anomaly_flags={p.get('anomaly_flags', [])}, "
            f"model={p.get('screening_model_version')}."
        ),
        "ComplianceCheckRequested": (
            f"Compliance checking initiated under {p.get('regulation_set_version')}: "
            f"required checks={p.get('checks_required', [])}."
        ),
        "ComplianceRulePassed": (
            f"Compliance rule {p.get('rule_id')} v{p.get('rule_version')} passed."
        ),
        "ComplianceRuleFailed": (
            f"Compliance rule {p.get('rule_id')} FAILED: {p.get('failure_reason')}."
        ),
        "ComplianceClearanceIssued": (
            f"Compliance clearance issued under {p.get('regulation_set_version')} "
            f"covering {p.get('checks_passed', [])}."
        ),
        "DecisionGenerated": (
            f"AI recommendation generated by orchestrator {p.get('orchestrator_agent_id')}: "
            f"{p.get('recommendation')} (confidence={p.get('confidence_score')}). "
            f"Contributing sessions: {p.get('contributing_agent_sessions', [])}."
        ),
        "HumanReviewCompleted": (
            f"Human review by {p.get('reviewer_id')}: {p.get('final_decision')}. "
            f"Override: {p.get('override', False)}."
            + (f" Reason: {p.get('override_reason')}" if p.get("override_reason") else "")
        ),
        "ApplicationApproved": (
            f"Application APPROVED for ${p.get('approved_amount_usd')} "
            f"by {p.get('approved_by')} effective {p.get('effective_date', 'immediately')}."
        ),
        "ApplicationDeclined": (
            f"Application DECLINED by {p.get('declined_by')}. "
            f"Reasons: {p.get('decline_reasons', [])}. "
            f"Adverse action notice required: {p.get('adverse_action_notice_required')}."
        ),
    }
    return summaries.get(event_type, f"{event_type}: {json.dumps(p, default=str)[:100]}")


def _extract_agent_metadata(loan_events, store) -> list[dict]:
    """Extract AI agent participation metadata from the event stream."""
    agents: dict[str, dict] = {}

    for event in loan_events:
        p = event.payload
        agent_id = p.get("agent_id") or p.get("orchestrator_agent_id")
        if not agent_id:
            continue

        if agent_id not in agents:
            agents[agent_id] = {
                "agent_id":         agent_id,
                "session_id":       p.get("session_id"),
                "model_version":    p.get("model_version"),
                "actions":          [],
                "confidence_scores": [],
                "input_data_hashes": [],
            }

        action = {
            "event_type":    event.event_type,
            "recorded_at":   event.recorded_at.isoformat(),
        }

        if p.get("confidence_score") is not None:
            agents[agent_id]["confidence_scores"].append(p["confidence_score"])
            action["confidence_score"] = p["confidence_score"]

        if p.get("input_data_hash"):
            agents[agent_id]["input_data_hashes"].append(p["input_data_hash"])
            action["input_data_hash"] = p["input_data_hash"]

        agents[agent_id]["actions"].append(action)

        # Update model version if available
        if p.get("model_version") and not agents[agent_id]["model_version"]:
            agents[agent_id]["model_version"] = p["model_version"]

        # Capture model_versions dict from DecisionGenerated
        if event.event_type == "DecisionGenerated" and p.get("model_versions"):
            agents[agent_id]["model_versions_used"] = p["model_versions"]

    return list(agents.values())
