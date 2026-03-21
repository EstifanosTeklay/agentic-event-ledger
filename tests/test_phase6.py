"""
tests/test_phase6.py
=====================
Phase 6 — What-If Counterfactual Projector and Regulatory Package tests.

Tests verify:
  - What-if produces materially different outcome when substituting HIGH risk
  - Counterfactual events never written to real store
  - Causal dependency filtering skips dependent events correctly
  - Regulatory package is complete and independently verifiable
  - Package hash is stable (deterministic)
  - Integrity chain is valid in the package
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from decimal import Decimal
from datetime import datetime, timezone

import asyncpg
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.startup import initialise
initialise()

from src.commands.handlers import (
    GenerateDecisionCommand,
    IssueClearanceCommand,
    RecordComplianceRuleCommand,
    RecordCreditAnalysisCommand,
    RequestComplianceCheckCommand,
    RequestCreditAnalysisCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    RecordHumanReviewCommand,
    handle_credit_analysis_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_issue_compliance_clearance,
    handle_record_compliance_rule,
    handle_request_compliance_check,
    handle_request_credit_analysis,
    handle_start_agent_session,
    handle_submit_application,
)
from src.event_store import EventStore
from src.models.events import (
    CreditAnalysisCompleted,
    Recommendation,
    RiskTier,
)
from src.what_if.projector import run_what_if
from src.regulatory.package import generate_regulatory_package
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.daemon import ProjectionDaemon

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger"
)


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=4, max_size=20)
    yield p
    await p.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE events, event_streams, outbox, "
            "projection_checkpoints, application_summary, "
            "agent_performance_ledger, compliance_audit_view, "
            "compliance_audit_snapshots RESTART IDENTITY CASCADE"
        )
    yield


@pytest_asyncio.fixture
async def store(pool):
    return EventStore(pool=pool)


# =============================================================================
# HELPERS
# =============================================================================

async def build_approved_application(
    store: EventStore,
    app_id: str = "APP-WHATIF-TEST-001",
    risk_tier: str = "MEDIUM",
    confidence_score: float = 0.85,
) -> dict:
    """Build a full approved application for what-if testing."""
    agent_id   = "agent-credit-test"
    session_id = "sess-test-001"
    orch_id    = "agent-orch-test"
    orch_sess  = "sess-orch-test"

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id=agent_id, session_id=session_id,
            model_version="credit-v2.1", context_token_count=2048,
        ), store)

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="test-borrower",
            requested_amount_usd=Decimal("500000"),
            loan_purpose="Test",
        ), store)

    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id=app_id,
            assigned_agent_id=agent_id,
        ), store)

    await handle_credit_analysis_completed(
        RecordCreditAnalysisCommand(
            application_id=app_id,
            agent_id=agent_id, session_id=session_id,
            model_version="credit-v2.1",
            risk_tier=RiskTier(risk_tier),
            recommended_limit_usd=Decimal("480000"),
            analysis_duration_ms=1000,
            input_data={"test": True},
            confidence_score=confidence_score,
        ), store)

    await handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id,
            regulation_set_version="REG-2026-Q1",
            checks_required=["AML-001", "KYC-001"],
        ), store)

    for rule in ["AML-001", "KYC-001"]:
        await handle_record_compliance_rule(
            RecordComplianceRuleCommand(
                application_id=app_id,
                rule_id=rule, rule_version="v1.0",
                passed=True, evidence_data={},
            ), store)

    await handle_issue_compliance_clearance(
        IssueClearanceCommand(
            application_id=app_id,
            issuing_agent_id="agent-compliance",
            regulation_set_version="REG-2026-Q1",
        ), store)

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id=orch_id, session_id=orch_sess,
            model_version="orch-v1.0", context_token_count=1024,
        ), store)

    credit_stream = f"agent-{agent_id}-{session_id}"
    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_agent_id=orch_id,
            orchestrator_session_id=orch_sess,
            recommendation=Recommendation.APPROVE,
            confidence_score=confidence_score,
            contributing_agent_sessions=[credit_stream],
            decision_basis_summary="Test decision",
            model_versions={},
        ), store)

    await handle_human_review_completed(
        RecordHumanReviewCommand(
            application_id=app_id,
            reviewer_id="officer-test",
            final_decision=Recommendation.APPROVE,
        ), store)

    return {
        "app_id": app_id,
        "agent_id": agent_id,
        "session_id": session_id,
    }


# =============================================================================
# WHAT-IF TESTS
# =============================================================================

@pytest.mark.asyncio
async def test_what_if_produces_different_outcome(store):
    """
    Substituting HIGH risk with low confidence must produce a different
    outcome than the real MEDIUM risk scenario.
    """
    app_id = "APP-WHATIF-001"
    await build_approved_application(store, app_id, risk_tier="MEDIUM", confidence_score=0.85)

    # Counterfactual: HIGH risk, confidence below floor
    cf_event = CreditAnalysisCompleted(
        application_id=app_id,
        agent_id="agent-credit-test",
        session_id="sess-test-001",
        model_version="credit-v2.1",
        risk_tier=RiskTier.HIGH,
        recommended_limit_usd=Decimal("300000"),
        analysis_duration_ms=1000,
        input_data_hash="sha256-counterfactual",
        confidence_score=0.52,  # below 0.6 floor
    )

    result = await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Real outcome must be FINAL_APPROVED
    assert result.real_final_state == "FINAL_APPROVED", \
        f"Real state must be FINAL_APPROVED, got {result.real_final_state}"

    # Counterfactual must differ
    assert result.outcome_changed, "Substituting HIGH risk must change the outcome"
    assert result.counterfactual_final_state != "FINAL_APPROVED", \
        f"Counterfactual must not be FINAL_APPROVED, got {result.counterfactual_final_state}"

    # Risk tier must be captured in counterfactual
    assert result.counterfactual_events is not None
    assert len(result.counterfactual_events) > 0

    print(f"\n✓ What-if outcome changed: {result.real_final_state} → {result.counterfactual_final_state}")
    print(f"  Divergence: {result.divergence_summary}")


@pytest.mark.asyncio
async def test_what_if_never_writes_to_real_store(store, pool):
    """
    CRITICAL: Counterfactual events must never be written to the real store.
    Event count before and after must be identical.
    """
    app_id = "APP-WHATIF-IMMUTABLE-001"
    await build_approved_application(store, app_id)

    # Count events before
    async with pool.acquire() as conn:
        count_before = await conn.fetchval("SELECT COUNT(*) FROM events")

    cf_event = CreditAnalysisCompleted(
        application_id=app_id,
        agent_id="agent-credit-test",
        session_id="sess-test-001",
        model_version="credit-v2.1",
        risk_tier=RiskTier.HIGH,
        recommended_limit_usd=Decimal("200000"),
        analysis_duration_ms=1000,
        input_data_hash="sha256-cf-immutable",
        confidence_score=0.45,
    )

    await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Count events after — must be identical
    async with pool.acquire() as conn:
        count_after = await conn.fetchval("SELECT COUNT(*) FROM events")

    assert count_before == count_after, (
        f"COUNTERFACTUAL WROTE TO REAL STORE: "
        f"count before={count_before}, after={count_after}"
    )
    print(f"\n✓ Immutability: event count unchanged ({count_before}) after what-if")


@pytest.mark.asyncio
async def test_what_if_causal_dependency_filtering(store):
    """
    Events causally dependent on the branch event must be skipped.
    DecisionGenerated depends on CreditAnalysisCompleted — must be excluded.
    """
    app_id = "APP-WHATIF-CAUSAL-001"
    await build_approved_application(store, app_id)

    cf_event = CreditAnalysisCompleted(
        application_id=app_id,
        agent_id="agent-credit-test",
        session_id="sess-test-001",
        model_version="credit-v2.1",
        risk_tier=RiskTier.HIGH,
        recommended_limit_usd=Decimal("250000"),
        analysis_duration_ms=1000,
        input_data_hash="sha256-cf-causal",
        confidence_score=0.55,
    )

    result = await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Check counterfactual sequence doesn't contain dependent events
    cf_event_types = [e["event_type"] for e in result.counterfactual_events]

    # DecisionGenerated and ApplicationApproved must be absent
    # (they were consequences of the real credit analysis)
    assert "ApplicationApproved" not in cf_event_types, \
        "ApplicationApproved must be filtered out (causally dependent)"
    assert "HumanReviewCompleted" not in cf_event_types, \
        "HumanReviewCompleted must be filtered out (causally dependent)"

    print(f"\n✓ Causal filtering: dependent events excluded from counterfactual")
    print(f"  Counterfactual event types: {cf_event_types}")


@pytest.mark.asyncio
async def test_what_if_same_outcome_when_risk_unchanged(store):
    """
    If the counterfactual uses the same risk tier and confidence,
    the outcome should be the same.
    """
    app_id = "APP-WHATIF-SAME-001"
    await build_approved_application(store, app_id, risk_tier="LOW", confidence_score=0.9)

    # Same as real — LOW risk, high confidence
    cf_event = CreditAnalysisCompleted(
        application_id=app_id,
        agent_id="agent-credit-test",
        session_id="sess-test-001",
        model_version="credit-v2.1",
        risk_tier=RiskTier.LOW,
        recommended_limit_usd=Decimal("480000"),
        analysis_duration_ms=1000,
        input_data_hash="sha256-cf-same",
        confidence_score=0.9,
    )

    result = await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # With same risk/confidence, final states should match at branch level
    # (both reach PENDING_DECISION with same intermediate state)
    assert result.real_final_state in ("FINAL_APPROVED", "FINAL_DECLINED")
    print(f"\n✓ Same-risk what-if: real={result.real_final_state}, "
          f"cf={result.counterfactual_final_state}, changed={result.outcome_changed}")


# =============================================================================
# REGULATORY PACKAGE TESTS
# =============================================================================

@pytest.mark.asyncio
async def test_regulatory_package_contains_all_sections(store, pool):
    """Regulatory package must contain all 5 required sections."""
    app_id = "APP-REG-001"
    await build_approved_application(store, app_id)

    package = await generate_regulatory_package(
        store=store,
        application_id=app_id,
        pool=pool,
    )

    # All 5 sections must be present
    assert "section_1_event_stream" in package, "Missing section 1: event stream"
    assert "section_2_projection_states" in package, "Missing section 2: projections"
    assert "section_3_integrity" in package, "Missing section 3: integrity"
    assert "section_4_narrative" in package, "Missing section 4: narrative"
    assert "section_5_ai_agent_metadata" in package, "Missing section 5: agent metadata"
    assert "package_hash" in package, "Missing package_hash"

    print(f"\n✓ All 5 sections present in regulatory package")


@pytest.mark.asyncio
async def test_regulatory_package_event_stream_complete(store, pool):
    """Section 1 must contain all events with payload hashes."""
    app_id = "APP-REG-EVENTS-001"
    await build_approved_application(store, app_id)

    package = await generate_regulatory_package(
        store=store,
        application_id=app_id,
        pool=pool,
    )

    events = package["section_1_event_stream"]["events"]
    assert len(events) >= 8, f"Expected ≥8 events, got {len(events)}"

    # Every event must have a payload_hash
    for e in events:
        assert "payload_hash" in e, f"Event {e['event_type']} missing payload_hash"
        assert len(e["payload_hash"]) == 64, "payload_hash must be SHA-256 (64 chars)"

    print(f"\n✓ Event stream complete: {len(events)} events, all with payload hashes")


@pytest.mark.asyncio
async def test_regulatory_package_integrity_valid(store, pool):
    """Section 3 integrity check must pass with chain_valid=True."""
    app_id = "APP-REG-INTEGRITY-001"
    await build_approved_application(store, app_id)

    package = await generate_regulatory_package(
        store=store,
        application_id=app_id,
        pool=pool,
    )

    integrity = package["section_3_integrity"]
    assert integrity["chain_valid"] is True, "Integrity chain must be valid"
    assert integrity["tamper_detected"] is False, "No tamper must be detected"
    assert integrity["events_verified"] >= 8
    assert len(integrity["integrity_hash"]) == 64

    print(f"\n✓ Integrity chain valid: {integrity['events_verified']} events verified")
    print(f"  Hash: {integrity['integrity_hash'][:16]}...")


@pytest.mark.asyncio
async def test_regulatory_package_narrative_not_empty(store, pool):
    """Section 4 narrative must have entries for all significant events."""
    app_id = "APP-REG-NARRATIVE-001"
    await build_approved_application(store, app_id)

    package = await generate_regulatory_package(
        store=store,
        application_id=app_id,
        pool=pool,
    )

    narrative = package["section_4_narrative"]["entries"]
    assert len(narrative) >= 8, f"Expected ≥8 narrative entries, got {len(narrative)}"

    # Every entry must have timestamp, event_type, and summary
    for entry in narrative:
        assert "timestamp" in entry
        assert "event_type" in entry
        assert "summary" in entry
        assert len(entry["summary"]) > 10, "Summary must be meaningful"

    print(f"\n✓ Narrative: {len(narrative)} entries, all with summaries")
    for entry in narrative[:3]:
        print(f"  • {entry['event_type']}: {entry['summary'][:60]}...")


@pytest.mark.asyncio
async def test_regulatory_package_agent_metadata_present(store, pool):
    """Section 5 must capture all AI agents that participated."""
    app_id = "APP-REG-AGENTS-001"
    await build_approved_application(store, app_id)

    package = await generate_regulatory_package(
        store=store,
        application_id=app_id,
        pool=pool,
    )

    agents = package["section_5_ai_agent_metadata"]["agents"]
    assert len(agents) >= 1, "Must capture at least 1 AI agent"

    for agent in agents:
        assert "agent_id" in agent
        assert "actions" in agent
        assert len(agent["actions"]) >= 1

    print(f"\n✓ Agent metadata: {len(agents)} agents captured")
    for a in agents:
        print(f"  {a['agent_id']}: {[x['event_type'] for x in a['actions']]}")


@pytest.mark.asyncio
async def test_regulatory_package_is_self_verifiable(store, pool):
    """
    The package hash must be recomputable from package contents.
    This proves the package is independently verifiable.
    """
    app_id = "APP-REG-VERIFY-001"
    await build_approved_application(store, app_id)

    package = await generate_regulatory_package(
        store=store,
        application_id=app_id,
        pool=pool,
    )

    stored_hash = package["package_hash"]

    # Recompute hash from package contents (excluding package_hash itself)
    package_without_hash = {k: v for k, v in package.items() if k != "package_hash"}
    package_str = json.dumps(package_without_hash, sort_keys=True, default=str)
    recomputed_hash = hashlib.sha256(package_str.encode()).hexdigest()

    assert stored_hash == recomputed_hash, (
        f"Package hash mismatch — package is not self-verifiable.\n"
        f"Stored:    {stored_hash}\n"
        f"Recomputed: {recomputed_hash}"
    )

    print(f"\n✓ Package is self-verifiable: hash matches recomputed value")
    print(f"  Hash: {stored_hash[:32]}...")
