"""
tests/test_mcp_lifecycle.py
============================
Phase 5 — Full loan application lifecycle driven entirely through MCP tools.

NO direct Python function calls after setup.
Every step uses MCP tool functions directly (simulating what an AI agent does).

Lifecycle:
  start_agent_session
  → submit_application
  → record_credit_analysis
  → record_fraud_screening
  → request_compliance_check
  → record_compliance_check (×2)
  → issue_compliance_clearance
  → start_agent_session (orchestrator)
  → generate_decision
  → record_human_review
  → query ledger://applications/{id}/audit-trail (verify complete trace)
  → query ledger://applications/{id}/compliance (verify all checks)
  → run_integrity_check (verify hash chain)
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import asyncpg
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.startup import initialise
initialise()

# Import MCP tool functions directly
from src.mcp.tools import (
    generate_decision,
    request_credit_analysis,
    issue_compliance_clearance,
    record_compliance_check,
    record_credit_analysis,
    record_fraud_screening,
    record_human_review,
    request_compliance_check,
    run_integrity_check_tool,
    start_agent_session,
    submit_application,
)
from src.mcp.resources import (
    get_application,
    get_application_compliance,
    get_audit_trail,
    get_ledger_health,
)
from src.mcp.dependencies import get_store, get_db_pool

# Override the store singleton for tests
from src.mcp import dependencies as _deps

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
    # Wire the test pool into the MCP dependencies
    from src.event_store import EventStore
    _deps._store = EventStore(pool=pool)
    yield
    _deps._store = None


# =============================================================================
# THE FULL LIFECYCLE TEST
# =============================================================================

@pytest.mark.asyncio
async def test_full_mcp_lifecycle(pool):
    """
    Complete loan application lifecycle driven entirely through MCP tools.
    No direct Python function calls after fixture setup.

    This test proves:
    1. All MCP tools work end-to-end
    2. The complete audit trail is queryable after final decision
    3. Compliance checks are all recorded in the compliance resource
    4. Cryptographic integrity check passes on the final stream
    """
    app_id      = "MCP-LIFECYCLE-001"
    agent_id    = "agent-credit-mcp-001"
    session_id  = "sess-mcp-001"
    orch_id     = "agent-orch-mcp-001"
    orch_sess   = "sess-orch-mcp-001"

    print(f"\n{'='*60}")
    print(f"  MCP LIFECYCLE TEST — {app_id}")
    print(f"{'='*60}")

    # ── Step 1: Start credit agent session ───────────────────────────────────
    result = await start_agent_session(
        agent_id=agent_id,
        session_id=session_id,
        model_version="credit-v2.1",
        context_source="fresh",
        context_token_count=4096,
    )
    assert result["status"] == "ok", f"start_agent_session failed: {result}"
    print(f"\n  [1] start_agent_session → {result['status']}")

    # ── Step 2: Submit application ───────────────────────────────────────────
    result = await submit_application(
        application_id=app_id,
        applicant_id="mcp-borrower-001",
        requested_amount_usd=650000.0,
        loan_purpose="Commercial warehouse acquisition",
        submission_channel="broker",
    )
    assert result["status"] == "ok", f"submit_application failed: {result}"
    assert result["stream_id"] == f"loan-{app_id}"
    print(f"  [2] submit_application → stream={result['stream_id']}")

    # ── Step 2b: Request credit analysis (AWAITING_ANALYSIS state) ──────────
    result = await request_credit_analysis(
        application_id=app_id,
        assigned_agent_id=agent_id,
        priority="HIGH",
    )
    assert result["status"] == "ok", f"request_credit_analysis failed: {result}"
    print(f"  [2b] request_credit_analysis → state={result['new_state']}")

    # ── Step 3: Record credit analysis ───────────────────────────────────────
    result = await record_credit_analysis(
        application_id=app_id,
        agent_id=agent_id,
        session_id=session_id,
        model_version="credit-v2.1",
        risk_tier="MEDIUM",
        recommended_limit_usd=620000.0,
        analysis_duration_ms=1180,
        confidence_score=0.87,
    )
    assert result["status"] == "ok", f"record_credit_analysis failed: {result}"
    print(f"  [3] record_credit_analysis → risk={result['risk_tier']}")

    # ── Step 4: Record fraud screening ───────────────────────────────────────
    result = await record_fraud_screening(
        application_id=app_id,
        agent_id=agent_id,
        session_id=session_id,
        fraud_score=0.09,
        screening_model_version="fraud-v3.1",
        anomaly_flags=[],
    )
    assert result["status"] == "ok", f"record_fraud_screening failed: {result}"
    print(f"  [4] record_fraud_screening → score={result['fraud_score']}, risk={result['risk_level']}")

    # ── Step 5: Request compliance check ─────────────────────────────────────
    result = await request_compliance_check(
        application_id=app_id,
        regulation_set_version="REG-2026-Q1",
        checks_required=["AML-001", "KYC-001"],
    )
    assert result["status"] == "ok", f"request_compliance_check failed: {result}"
    print(f"  [5] request_compliance_check → {result['checks_required']}")

    # ── Step 6: Record AML check ─────────────────────────────────────────────
    result = await record_compliance_check(
        application_id=app_id,
        rule_id="AML-001",
        rule_version="v2.1",
        passed=True,
    )
    assert result["status"] == "ok", f"record_compliance_check AML failed: {result}"
    print(f"  [6] record_compliance_check AML-001 → {result['compliance_status']}")

    # ── Step 7: Record KYC check ─────────────────────────────────────────────
    result = await record_compliance_check(
        application_id=app_id,
        rule_id="KYC-001",
        rule_version="v1.8",
        passed=True,
    )
    assert result["status"] == "ok", f"record_compliance_check KYC failed: {result}"
    print(f"  [7] record_compliance_check KYC-001 → {result['compliance_status']}")

    # ── Step 8: Issue compliance clearance ───────────────────────────────────
    result = await issue_compliance_clearance(
        application_id=app_id,
        issuing_agent_id="agent-compliance-001",
        regulation_set_version="REG-2026-Q1",
    )
    assert result["status"] == "ok", f"issue_compliance_clearance failed: {result}"
    assert result["clearance_issued"] is True
    print(f"  [8] issue_compliance_clearance → clearance_issued={result['clearance_issued']}")

    # ── Step 9: Start orchestrator session ───────────────────────────────────
    result = await start_agent_session(
        agent_id=orch_id,
        session_id=orch_sess,
        model_version="orch-v1.0",
        context_source="fresh",
        context_token_count=2048,
    )
    assert result["status"] == "ok"
    print(f"  [9] start_agent_session (orchestrator) → {result['status']}")

    # ── Step 10: Generate decision ───────────────────────────────────────────
    credit_session_stream = f"agent-{agent_id}-{session_id}"
    result = await generate_decision(
        application_id=app_id,
        orchestrator_agent_id=orch_id,
        orchestrator_session_id=orch_sess,
        recommendation="APPROVE",
        confidence_score=0.91,
        contributing_agent_sessions=[credit_session_stream],
        decision_basis_summary="Strong financials, low fraud risk, full compliance",
        model_versions={"orchestrator": "orch-v1.0", "credit": "credit-v2.1"},
    )
    assert result["status"] == "ok", f"generate_decision failed: {result}"
    print(f"  [10] generate_decision → {result['recommendation']} (confidence={result['confidence_score']})")

    # ── Step 11: Record human review ─────────────────────────────────────────
    result = await record_human_review(
        application_id=app_id,
        reviewer_id="officer-mcp-001",
        final_decision="APPROVE",
        override=False,
    )
    assert result["status"] == "ok", f"record_human_review failed: {result}"
    assert result["application_state"] == "FINAL_APPROVED"
    print(f"  [11] record_human_review → state={result['application_state']}")

    # ── Step 12: Run projection daemon to update read models ─────────────────
    from src.projections.application_summary import ApplicationSummaryProjection
    from src.projections.agent_performance import AgentPerformanceLedgerProjection
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    from src.projections.daemon import ProjectionDaemon
    from src.event_store import EventStore

    store = EventStore(pool=pool)
    daemon = ProjectionDaemon(
        store=store,
        projections=[
            ApplicationSummaryProjection(),
            AgentPerformanceLedgerProjection(),
            ComplianceAuditViewProjection(pool=pool),
        ],
        pool=pool,
    )
    await daemon._initialise_checkpoints(pool)
    await daemon._process_batch(pool)
    await daemon._process_batch(pool)

    # ── Step 13: Query audit trail (THE WEEK STANDARD) ───────────────────────
    print(f"\n  {'─'*50}")
    print(f"  QUERYING: ledger://applications/{app_id}/audit-trail")
    print(f"  {'─'*50}")

    audit_json = await get_audit_trail(application_id=app_id)
    audit = json.loads(audit_json)

    assert "error" not in audit, f"Audit trail error: {audit}"
    assert audit["data"]["total_events"] >= 8, \
        f"Expected ≥8 events, got {audit['data']['total_events']}"

    narrative = audit["data"]["narrative"]
    assert len(narrative) >= 5, "Narrative must have entries for key events"

    print(f"  Total events: {audit['data']['total_events']}")
    print(f"  Narrative ({len(narrative)} entries):")
    for line in narrative:
        print(f"    • {line}")

    # ── Step 14: Query compliance resource ───────────────────────────────────
    print(f"\n  {'─'*50}")
    print(f"  QUERYING: ledger://applications/{app_id}/compliance")
    print(f"  {'─'*50}")

    comp_json = await get_application_compliance(application_id=app_id)
    comp = json.loads(comp_json)

    assert "error" not in comp, f"Compliance resource error: {comp}"
    assert comp["data"]["passed"] >= 2, \
        f"Expected ≥2 passed checks, got {comp['data']['passed']}"

    print(f"  Total checks: {comp['data']['total_checks']}")
    print(f"  Passed: {comp['data']['passed']}")
    print(f"  Failed: {comp['data']['failed']}")
    print(f"  Pending: {comp['data']['pending']}")

    # ── Step 15: Run integrity check ─────────────────────────────────────────
    print(f"\n  {'─'*50}")
    print(f"  RUNNING: run_integrity_check LoanApplication/{app_id}")
    print(f"  {'─'*50}")

    integrity = await run_integrity_check_tool(
        entity_type="LoanApplication",
        entity_id=app_id,
    )
    assert integrity["status"] == "ok", f"Integrity check failed: {integrity}"
    assert integrity["chain_valid"] is True
    assert integrity["tamper_detected"] is False

    print(f"  Events verified: {integrity['events_verified']}")
    print(f"  Chain valid: {integrity['chain_valid']}")
    print(f"  Tamper detected: {integrity['tamper_detected']}")
    print(f"  Integrity hash: {integrity['integrity_hash']}")

    # ── Step 16: Query health endpoint ───────────────────────────────────────
    health_json = await get_ledger_health()
    health = json.loads(health_json)
    print(f"\n  Ledger health: {health['status']}")
    print(f"  Total events in store: {health['total_events_in_store']}")

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  MCP LIFECYCLE TEST — ALL STEPS PASSED")
    print(f"{'='*60}")
    print(f"  Application {app_id}: FINAL_APPROVED ✓")
    print(f"  Total events: {audit['data']['total_events']} ✓")
    print(f"  Compliance: {comp['data']['passed']} rules passed ✓")
    print(f"  Chain valid: {integrity['chain_valid']} ✓")
    print(f"  All MCP tools: functional ✓")
    print(f"  All MCP resources: functional ✓")
    print(f"{'='*60}")
