"""
src/aggregates/loan_application.py
===================================
LoanApplicationAggregate — the central aggregate in the Apex Financial scenario.

Responsibilities:
  - Enforces the application state machine (8 states, valid transitions only)
  - Business Rule 1: Valid state transitions only
  - Business Rule 3: Model version locking on credit analysis
  - Business Rule 4: Confidence floor forces REFER recommendation
  - Business Rule 5: Approval requires compliance clearance
  - Business Rule 6: Causal chain — contributing sessions must have processed this app

State Machine:
  SUBMITTED
    → AWAITING_ANALYSIS       (on CreditAnalysisRequested)
    → ANALYSIS_COMPLETE       (on CreditAnalysisCompleted)
    → COMPLIANCE_REVIEW       (on ComplianceCheckRequested)
    → PENDING_DECISION        (on ComplianceClearanceIssued)
    → APPROVED_PENDING_HUMAN  (on DecisionGenerated with APPROVE/REFER)
    → DECLINED_PENDING_HUMAN  (on DecisionGenerated with DECLINE)
    → FINAL_APPROVED          (on ApplicationApproved)
    → FINAL_DECLINED          (on ApplicationDeclined)
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from src.models.events import (
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationState,
    ApplicationSubmitted,
    ApplicationUnderReview,
    ComplianceClearanceIssued,
    ComplianceCheckRequested,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    CreditAnalysisSuperseded,
    DecisionGenerated,
    DomainError,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    Recommendation,
    RiskTier,
    StoredEvent,
)

if TYPE_CHECKING:
    from src.event_store import EventStore

logger = logging.getLogger(__name__)

# Valid state transitions: current_state → set of allowed next states
VALID_TRANSITIONS: dict[ApplicationState, set[ApplicationState]] = {
    ApplicationState.SUBMITTED: {
        ApplicationState.AWAITING_ANALYSIS,
    },
    ApplicationState.AWAITING_ANALYSIS: {
        ApplicationState.ANALYSIS_COMPLETE,
    },
    ApplicationState.ANALYSIS_COMPLETE: {
        ApplicationState.COMPLIANCE_REVIEW,
    },
    ApplicationState.COMPLIANCE_REVIEW: {
        ApplicationState.PENDING_DECISION,
    },
    ApplicationState.PENDING_DECISION: {
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
    },
    ApplicationState.APPROVED_PENDING_HUMAN: {
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,  # human may override approve→decline
    },
    ApplicationState.DECLINED_PENDING_HUMAN: {
        ApplicationState.FINAL_DECLINED,
        ApplicationState.FINAL_APPROVED,  # human may override decline→approve
    },
    # Terminal states — no transitions out
    ApplicationState.FINAL_APPROVED: set(),
    ApplicationState.FINAL_DECLINED: set(),
}


class LoanApplicationAggregate:
    """
    Reconstructed from event stream. Never mutate directly — always append events.

    Usage:
        app = await LoanApplicationAggregate.load(store, application_id)
        app.assert_awaiting_credit_analysis()
        # ... then append new events via store.append(...)
    """

    def __init__(self, application_id: str) -> None:
        self.application_id = application_id
        self.version: int = 0

        # State machine
        self.state: ApplicationState = ApplicationState.SUBMITTED

        # Domain data populated by event replay
        self.applicant_id: str | None = None
        self.requested_amount_usd: Decimal | None = None
        self.approved_amount_usd: Decimal | None = None
        self.loan_purpose: str | None = None

        # Agent session tracking
        self.assigned_agent_id: str | None = None
        self.credit_analysis_session_id: str | None = None
        self.credit_analysis_superseded: bool = False
        self.risk_tier: RiskTier | None = None
        self.recommended_limit_usd: Decimal | None = None
        self.fraud_score: float | None = None

        # Compliance tracking
        self.compliance_cleared: bool = False
        self.compliance_checks_required: list[str] = []
        self.regulation_set_version: str | None = None

        # Decision tracking
        self.recommendation: Recommendation | None = None
        self.decision_confidence: float | None = None
        self.contributing_sessions: list[str] = []
        self.orchestrator_agent_id: str | None = None

        # Human review
        self.human_reviewer_id: str | None = None
        self.final_decision: Recommendation | None = None
        self.human_override: bool = False

    # -------------------------------------------------------------------------
    # FACTORY — Reconstruct from event stream
    # -------------------------------------------------------------------------

    @classmethod
    async def load(
        cls,
        store: "EventStore",
        application_id: str,
        to_position: int | None = None,
    ) -> "LoanApplicationAggregate":
        """
        Reconstruct aggregate state by replaying events from the loan stream.
        Applies upcasting transparently via EventStore.load_stream().

        Args:
            store:          EventStore instance.
            application_id: The application UUID.
            to_position:    Optional — replay only up to this stream position
                            (enables temporal queries).
        """
        from src.event_store import EventStore  # avoid circular at module level

        stream_id = f"loan-{application_id}"
        events = await store.load_stream(
            stream_id,
            to_position=to_position,
        )
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    # -------------------------------------------------------------------------
    # EVENT APPLICATION (one method per event type — explicit, not dynamic)
    # -------------------------------------------------------------------------

    def _apply(self, event: StoredEvent) -> None:
        """Route event to its specific handler. Unknown events are silently skipped."""
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)
        else:
            logger.debug("No handler for event type '%s' on LoanApplication", event.event_type)
        self.version = event.stream_position

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload["applicant_id"]
        self.requested_amount_usd = Decimal(str(event.payload["requested_amount_usd"]))
        self.loan_purpose = event.payload.get("loan_purpose")

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self._transition_to(ApplicationState.AWAITING_ANALYSIS)
        self.assigned_agent_id = event.payload["assigned_agent_id"]

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self._transition_to(ApplicationState.ANALYSIS_COMPLETE)
        self.credit_analysis_session_id = event.payload.get("session_id")
        self.risk_tier = RiskTier(event.payload["risk_tier"])
        self.recommended_limit_usd = Decimal(str(event.payload["recommended_limit_usd"]))

    def _on_CreditAnalysisSuperseded(self, event: StoredEvent) -> None:
        # Allows a second credit analysis after human override (Business Rule 3)
        self.credit_analysis_superseded = True
        self.credit_analysis_session_id = None

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.fraud_score = event.payload["fraud_score"]

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self._transition_to(ApplicationState.COMPLIANCE_REVIEW)
        self.compliance_checks_required = event.payload.get("checks_required", [])
        self.regulation_set_version = event.payload.get("regulation_set_version")

    def _on_ComplianceClearanceIssued(self, event: StoredEvent) -> None:
        self._transition_to(ApplicationState.PENDING_DECISION)
        self.compliance_cleared = True

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        recommendation = Recommendation(event.payload["recommendation"])
        if recommendation in (Recommendation.APPROVE, Recommendation.REFER):
            self._transition_to(ApplicationState.APPROVED_PENDING_HUMAN)
        else:
            self._transition_to(ApplicationState.DECLINED_PENDING_HUMAN)

        self.recommendation = recommendation
        self.decision_confidence = event.payload.get("confidence_score")
        self.contributing_sessions = event.payload.get("contributing_agent_sessions", [])
        self.orchestrator_agent_id = event.payload.get("orchestrator_agent_id")

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        self.human_reviewer_id = event.payload["reviewer_id"]
        self.human_override = event.payload.get("override", False)
        self.final_decision = Recommendation(event.payload["final_decision"])

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self._transition_to(ApplicationState.FINAL_APPROVED)
        self.approved_amount_usd = Decimal(str(event.payload["approved_amount_usd"]))

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self._transition_to(ApplicationState.FINAL_DECLINED)

    # -------------------------------------------------------------------------
    # STATE MACHINE ENFORCEMENT
    # -------------------------------------------------------------------------

    def _transition_to(self, new_state: ApplicationState) -> None:
        """
        Business Rule 1: Only valid transitions are permitted.
        Raises DomainError for any out-of-order transition.
        """
        allowed = VALID_TRANSITIONS.get(self.state, set())
        if new_state not in allowed:
            raise DomainError(
                f"Invalid state transition: {self.state.value} → {new_state.value}. "
                f"Allowed from {self.state.value}: "
                f"{[s.value for s in allowed] or 'none (terminal state)'}",
                rule_name="ApplicationStateMachine",
            )
        self.state = new_state

    # -------------------------------------------------------------------------
    # ASSERTION HELPERS — called by command handlers before appending
    # -------------------------------------------------------------------------

    def assert_in_state(self, *states: ApplicationState) -> None:
        """Raise DomainError if not in one of the expected states."""
        if self.state not in states:
            raise DomainError(
                f"Application {self.application_id} is in state '{self.state.value}', "
                f"expected one of: {[s.value for s in states]}",
                rule_name="ApplicationStateAssertion",
            )

    def assert_awaiting_credit_analysis(self) -> None:
        self.assert_in_state(ApplicationState.AWAITING_ANALYSIS)

    def assert_no_prior_credit_analysis(self) -> None:
        """
        Business Rule 3: No second credit analysis unless first was superseded.
        Raises DomainError if a credit analysis already exists for this app.
        """
        if (
            self.credit_analysis_session_id is not None
            and not self.credit_analysis_superseded
        ):
            raise DomainError(
                f"Application {self.application_id} already has a credit analysis "
                f"from session '{self.credit_analysis_session_id}'. "
                "A CreditAnalysisSuperseded event is required before re-analysis.",
                rule_name="ModelVersionLocking",
            )

    def assert_compliance_cleared(self) -> None:
        """
        Business Rule 5: Approval requires compliance clearance.
        Raises DomainError if compliance has not been cleared.
        """
        if not self.compliance_cleared:
            raise DomainError(
                f"Application {self.application_id} cannot be approved: "
                "compliance clearance has not been issued. "
                "All ComplianceRulePassed checks must complete first.",
                rule_name="ComplianceDependency",
            )

    def assert_not_terminal(self) -> None:
        """Raise DomainError if application is in a terminal state."""
        terminal = {ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED}
        if self.state in terminal:
            raise DomainError(
                f"Application {self.application_id} is in terminal state "
                f"'{self.state.value}'. No further events may be appended.",
                rule_name="TerminalStateViolation",
            )

    def assert_valid_contributing_sessions(
        self,
        contributing_sessions: list[str],
        known_application_sessions: set[str],
    ) -> None:
        """
        Business Rule 6: Every contributing session must have processed this app.
        The orchestrator cannot reference sessions that never touched this application.
        """
        invalid = set(contributing_sessions) - known_application_sessions
        if invalid:
            raise DomainError(
                f"DecisionGenerated references agent sessions that never processed "
                f"application {self.application_id}: {invalid}. "
                "Causal chain integrity violated.",
                rule_name="CausalChainEnforcement",
            )

    @staticmethod
    def enforce_confidence_floor(
        recommendation: Recommendation,
        confidence_score: float,
    ) -> Recommendation:
        """
        Business Rule 4: confidence_score < 0.6 forces REFER regardless of
        orchestrator recommendation. Regulatory requirement — enforced here in
        the domain layer, not in the API or MCP layer.
        """
        if confidence_score < 0.6 and recommendation != Recommendation.REFER:
            logger.warning(
                "Confidence floor enforced: score %.3f < 0.6, "
                "overriding %s → REFER",
                confidence_score,
                recommendation.value,
            )
            return Recommendation.REFER
        return recommendation
