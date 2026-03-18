"""
tests/test_aggregates.py
========================
Phase 2 — Aggregate state machine and all 6 business rules.

Tests verify that business rules are enforced in the domain layer (aggregates),
not the API layer. Rules are tested by constructing scenarios that should
succeed and scenarios that should raise DomainError.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.commands.handlers import (
    FinaliseApplicationCommand,
    GenerateDecisionCommand,
    IssueClearanceCommand,
    RecordComplianceRuleCommand,
    RecordCreditAnalysisCommand,
    RecordFraudScreeningCommand,
    RecordHumanReviewCommand,
    RequestComplianceCheckCommand,
    RequestCreditAnalysisCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
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
    ApplicationState,
    DomainError,
    OptimisticConcurrencyError,
    Recommendation,
    RiskTier,
)

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger"
)


@pytest_asyncio.fixture(scope="session")
async def pool():
    p = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    yield p
    await p.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE events, event_streams, outbox, "
            "projection_checkpoints RESTART IDENTITY CASCADE"
        )
    yield


@pytest_asyncio.fixture
async def store(pool):
    return EventStore(pool=pool)


# =============================================================================
# HELPERS
# =============================================================================

async def full_application_lifecycle(
    store: EventStore,
    app_id: str = "APP-001",
    agent_id: str = "agent-credit-001",
    session_id: str = "sess-001",
) -> dict:
    """Drive an application through the full happy-path lifecycle."""
    # Submit
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="applicant-001",
            requested_amount_usd=Decimal("300000"),
            loan_purpose="Working capital",
        ),
        store,
    )

    # Request credit analysis
    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id=app_id,
            assigned_agent_id=agent_id,
        ),
        store,
    )

    # Start agent session (Gas Town)
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id=agent_id,
            session_id=session_id,
            model_version="credit-v2.1",
            context_token_count=4096,
        ),
        store,
    )

    # Credit analysis
    await handle_credit_analysis_completed(
        RecordCreditAnalysisCommand(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            model_version="credit-v2.1",
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("280000"),
            analysis_duration_ms=1200,
            input_data={"financials": "..."}, 
            confidence_score=0.88,
        ),
        store,
    )

    # Compliance check
    await handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id,
            regulation_set_version="REG-2026-Q1",
            checks_required=["AML-001", "KYC-001"],
        ),
        store,
    )

    for rule_id in ["AML-001", "KYC-001"]:
        await handle_record_compliance_rule(
            RecordComplianceRuleCommand(
                application_id=app_id,
                rule_id=rule_id,
                rule_version="v1.0",
                passed=True,
                evidence_data={"result": "pass"},
            ),
            store,
        )

    # Issue clearance
    await handle_issue_compliance_clearance(
        IssueClearanceCommand(
            application_id=app_id,
            issuing_agent_id="agent-compliance-001",
            regulation_set_version="REG-2026-Q1",
        ),
        store,
    )

    return {
        "app_id": app_id,
        "agent_id": agent_id,
        "session_id": session_id,
        "agent_stream": f"agent-{agent_id}-{session_id}",
    }


# =============================================================================
# HAPPY PATH
# =============================================================================

@pytest.mark.asyncio
async def test_full_lifecycle_happy_path(store):
    """Full application lifecycle from submission to final decision."""
    ctx = await full_application_lifecycle(store)
    app_id = ctx["app_id"]
    agent_id = ctx["agent_id"]
    session_id = ctx["session_id"]

    orch_session = "sess-orch-001"
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-orch-001",
            session_id=orch_session,
            model_version="orch-v1.0",
            context_token_count=2048,
        ),
        store,
    )

    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_agent_id="agent-orch-001",
            orchestrator_session_id=orch_session,
            recommendation=Recommendation.APPROVE,
            confidence_score=0.91,
            contributing_agent_sessions=[f"agent-{agent_id}-{session_id}"],
            decision_basis_summary="Strong financials, low fraud risk",
            model_versions={"orchestrator": "orch-v1.0", "credit": "credit-v2.1"},
        ),
        store,
    )

    await handle_human_review_completed(
        RecordHumanReviewCommand(
            application_id=app_id,
            reviewer_id="officer-001",
            final_decision=Recommendation.APPROVE,
            override=False,
        ),
        store,
    )

    from src.aggregates.loan_application import LoanApplicationAggregate
    app = await LoanApplicationAggregate.load(store, app_id)
    assert app.state == ApplicationState.FINAL_APPROVED
    print(f"\n✓ Full lifecycle: {app_id} → {app.state.value}")


# =============================================================================
# BUSINESS RULE 1: State machine transitions
# =============================================================================

@pytest.mark.asyncio
async def test_rule1_invalid_state_transition(store):
    """Cannot skip states — credit analysis requires AWAITING_ANALYSIS state."""
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id="APP-RULE1",
            applicant_id="applicant-001",
            requested_amount_usd=Decimal("100000"),
            loan_purpose="Test",
        ),
        store,
    )

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-x", session_id="sess-x",
            model_version="v1", context_token_count=100,
        ),
        store,
    )

    # Try to complete credit analysis without first requesting it
    # App is still SUBMITTED, not AWAITING_ANALYSIS
    with pytest.raises(DomainError) as exc:
        await handle_credit_analysis_completed(
            RecordCreditAnalysisCommand(
                application_id="APP-RULE1",
                agent_id="agent-x",
                session_id="sess-x",
                model_version="v1",
                risk_tier=RiskTier.LOW,
                recommended_limit_usd=Decimal("100000"),
                analysis_duration_ms=500,
                input_data={},
                confidence_score=0.9,
            ),
            store,
        )

    assert "state" in str(exc.value).lower() or "SUBMITTED" in str(exc.value)
    print(f"\n✓ Rule 1: State machine prevents out-of-order transition")


@pytest.mark.asyncio
async def test_rule1_terminal_state_blocks_further_events(store):
    """Terminal states (FINAL_APPROVED) reject all further transitions."""
    ctx = await full_application_lifecycle(store, app_id="APP-TERMINAL")

    orch_sess = "orch-sess-001"
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-orch", session_id=orch_sess,
            model_version="orch-v1", context_token_count=1024,
        ),
        store,
    )

    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id="APP-TERMINAL",
            orchestrator_agent_id="agent-orch",
            orchestrator_session_id=orch_sess,
            recommendation=Recommendation.APPROVE,
            confidence_score=0.85,
            contributing_agent_sessions=[ctx["agent_stream"]],
            decision_basis_summary="Approved",
            model_versions={},
        ),
        store,
    )

    await handle_human_review_completed(
        RecordHumanReviewCommand(
            application_id="APP-TERMINAL",
            reviewer_id="officer-001",
            final_decision=Recommendation.APPROVE,
        ),
        store,
    )

    # Now try to submit another review on a terminal state
    with pytest.raises(DomainError):
        await handle_human_review_completed(
            RecordHumanReviewCommand(
                application_id="APP-TERMINAL",
                reviewer_id="officer-002",
                final_decision=Recommendation.DECLINE,
            ),
            store,
        )
    print("\n✓ Rule 1: Terminal state blocks further events")


# =============================================================================
# BUSINESS RULE 2: Gas Town — context must be loaded
# =============================================================================

@pytest.mark.asyncio
async def test_rule2_gas_town_no_context_loaded(store):
    """An agent without AgentContextLoaded cannot make decisions."""
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id="APP-GASTTOWN",
            applicant_id="applicant-001",
            requested_amount_usd=Decimal("100000"),
            loan_purpose="Test",
        ),
        store,
    )

    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id="APP-GASTTOWN",
            assigned_agent_id="agent-no-ctx",
        ),
        store,
    )

    # Start a session WITHOUT the AgentContextLoaded — should fail because
    # we never called handle_start_agent_session for this agent
    with pytest.raises(Exception):  # StreamNotFoundError or DomainError
        await handle_credit_analysis_completed(
            RecordCreditAnalysisCommand(
                application_id="APP-GASTTOWN",
                agent_id="agent-no-ctx",
                session_id="sess-no-ctx",
                model_version="v1",
                risk_tier=RiskTier.LOW,
                recommended_limit_usd=Decimal("100000"),
                analysis_duration_ms=500,
                input_data={},
            ),
            store,
        )
    print("\n✓ Rule 2 (Gas Town): Agent without context cannot make decisions")


# =============================================================================
# BUSINESS RULE 3: Model version locking
# =============================================================================

@pytest.mark.asyncio
async def test_rule3_model_version_mismatch_rejected(store):
    """A decision using a different model version than session declaration is rejected."""
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id="APP-MODELVER",
            applicant_id="applicant-001",
            requested_amount_usd=Decimal("200000"),
            loan_purpose="Test",
        ),
        store,
    )
    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id="APP-MODELVER",
            assigned_agent_id="agent-mv",
        ),
        store,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-mv", session_id="sess-mv",
            model_version="credit-v2.0",   # declares v2.0
            context_token_count=1024,
        ),
        store,
    )

    with pytest.raises(DomainError) as exc:
        await handle_credit_analysis_completed(
            RecordCreditAnalysisCommand(
                application_id="APP-MODELVER",
                agent_id="agent-mv",
                session_id="sess-mv",
                model_version="credit-v2.1",   # uses v2.1 — mismatch!
                risk_tier=RiskTier.LOW,
                recommended_limit_usd=Decimal("200000"),
                analysis_duration_ms=1000,
                input_data={},
                confidence_score=0.9,
            ),
            store,
        )

    assert "ModelVersionLocking" in exc.value.rule_name
    print("\n✓ Rule 3: Model version mismatch rejected")


# =============================================================================
# BUSINESS RULE 4: Confidence floor
# =============================================================================

@pytest.mark.asyncio
async def test_rule4_confidence_floor_forces_refer(store):
    """confidence_score < 0.6 forces REFER regardless of orchestrator recommendation."""
    from src.aggregates.loan_application import LoanApplicationAggregate

    # Test the pure function directly
    result = LoanApplicationAggregate.enforce_confidence_floor(
        Recommendation.APPROVE, confidence_score=0.45
    )
    assert result == Recommendation.REFER, f"Expected REFER, got {result}"

    result = LoanApplicationAggregate.enforce_confidence_floor(
        Recommendation.DECLINE, confidence_score=0.55
    )
    assert result == Recommendation.REFER

    # High confidence — no override
    result = LoanApplicationAggregate.enforce_confidence_floor(
        Recommendation.APPROVE, confidence_score=0.75
    )
    assert result == Recommendation.APPROVE

    print("\n✓ Rule 4: Confidence floor correctly forces REFER at <0.6")


# =============================================================================
# BUSINESS RULE 5: Compliance dependency
# =============================================================================

@pytest.mark.asyncio
async def test_rule5_cannot_approve_without_compliance(store):
    """Compliance clearance cannot be issued with failing or missing checks."""
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id="APP-COMPLIANCE",
            applicant_id="applicant-001",
            requested_amount_usd=Decimal("150000"),
            loan_purpose="Test",
        ),
        store,
    )
    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id="APP-COMPLIANCE",
            assigned_agent_id="agent-c",
        ),
        store,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-c", session_id="sess-c",
            model_version="v1", context_token_count=1024,
        ),
        store,
    )
    await handle_credit_analysis_completed(
        RecordCreditAnalysisCommand(
            application_id="APP-COMPLIANCE",
            agent_id="agent-c", session_id="sess-c",
            model_version="v1", risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("150000"),
            analysis_duration_ms=500, input_data={},
            confidence_score=0.9,
        ),
        store,
    )
    await handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id="APP-COMPLIANCE",
            regulation_set_version="REG-2026",
            checks_required=["AML-001", "KYC-001"],
        ),
        store,
    )

    # Only pass ONE of two required rules
    await handle_record_compliance_rule(
        RecordComplianceRuleCommand(
            application_id="APP-COMPLIANCE",
            rule_id="AML-001", rule_version="v1",
            passed=True, evidence_data={},
        ),
        store,
    )

    # Try to issue clearance with KYC-001 still pending → must fail
    with pytest.raises(DomainError) as exc:
        await handle_issue_compliance_clearance(
            IssueClearanceCommand(
                application_id="APP-COMPLIANCE",
                issuing_agent_id="agent-compliance",
                regulation_set_version="REG-2026",
            ),
            store,
        )

    assert "ComplianceDependency" in exc.value.rule_name
    print("\n✓ Rule 5: Compliance clearance blocked with pending checks")


# =============================================================================
# BUSINESS RULE 6: Causal chain enforcement
# =============================================================================

@pytest.mark.asyncio
async def test_rule6_contributing_session_must_have_processed_app(store):
    """DecisionGenerated cannot reference a session that didn't process the application."""
    ctx = await full_application_lifecycle(store, app_id="APP-CAUSAL")

    orch_sess = "orch-sess-causal"
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-orch-causal", session_id=orch_sess,
            model_version="orch-v1", context_token_count=1024,
        ),
        store,
    )

    # Reference a completely unrelated session
    fake_session = "agent-fake-agent-fake-session"

    with pytest.raises(DomainError) as exc:
        await handle_generate_decision(
            GenerateDecisionCommand(
                application_id="APP-CAUSAL",
                orchestrator_agent_id="agent-orch-causal",
                orchestrator_session_id=orch_sess,
                recommendation=Recommendation.APPROVE,
                confidence_score=0.88,
                contributing_agent_sessions=[fake_session],  # ← fake session
                decision_basis_summary="Test",
                model_versions={},
            ),
            store,
        )

    assert "CausalChainEnforcement" in exc.value.rule_name
    print("\n✓ Rule 6: Causal chain — fake contributing session rejected")
