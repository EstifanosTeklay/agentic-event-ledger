"""
src/aggregates/agent_session.py
================================
AgentSessionAggregate — tracks a single AI agent's work session.

Key invariants enforced:
  - Business Rule 2 (Gas Town): AgentContextLoaded MUST be the first event.
    No decision event may precede it. An agent without declared context
    is an agent without auditable provenance.
  - Business Rule 3 (Model Version Locking): Once a model version is established
    in the session, subsequent decision events must use the same version.
  - Every output event must reference the context loaded at session start.

The Gas Town pattern implemented here:
  On crash/restart → reconstruct_agent_context() in src/integrity/gas_town.py
  loads this aggregate's event stream and rebuilds the agent's working memory.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.models.events import (
    AgentContextLoaded,
    AgentSessionClosed,
    CreditAnalysisCompleted,
    DomainError,
    FraudScreeningCompleted,
    SessionHealthStatus,
    StoredEvent,
)

if TYPE_CHECKING:
    from src.event_store import EventStore

logger = logging.getLogger(__name__)


class AgentSessionAggregate:
    """
    Tracks the full lifecycle of one AI agent's session.

    Stream ID: agent-{agent_id}-{session_id}

    Lifecycle:
      1. AgentContextLoaded  (REQUIRED first event — Gas Town)
      2. CreditAnalysisCompleted | FraudScreeningCompleted | ...
      3. AgentSessionClosed  (optional — explicit session termination)
    """

    def __init__(self, agent_id: str, session_id: str) -> None:
        self.agent_id = agent_id
        self.session_id = session_id
        self.version: int = 0

        # Gas Town state
        self.context_loaded: bool = False
        self.context_source: str | None = None
        self.context_token_count: int = 0
        self.context_loaded_at: datetime | None = None
        self.event_replay_from_position: int = 0

        # Model version locking
        self.model_version: str | None = None
        self.established_model_version: str | None = None

        # Work tracking
        self.applications_processed: list[str] = []  # application_ids
        self.decisions_made: list[str] = []           # application_ids with decisions
        self.analyses_completed: int = 0
        self.screenings_completed: int = 0

        # Session state
        self.is_closed: bool = False
        self.close_reason: str | None = None
        self.health_status: SessionHealthStatus = SessionHealthStatus.HEALTHY

        # Last event tracking (for Gas Town reconciliation)
        self.last_event_type: str | None = None
        self.last_event_position: int = 0

    # -------------------------------------------------------------------------
    # FACTORY
    # -------------------------------------------------------------------------

    @classmethod
    async def load(
        cls,
        store: "EventStore",
        agent_id: str,
        session_id: str,
    ) -> "AgentSessionAggregate":
        """Reconstruct agent session state from event stream."""
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await store.load_stream(stream_id)
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        return agg

    @classmethod
    def stream_id_for(cls, agent_id: str, session_id: str) -> str:
        return f"agent-{agent_id}-{session_id}"

    # -------------------------------------------------------------------------
    # EVENT APPLICATION
    # -------------------------------------------------------------------------

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)
        self.last_event_type = event.event_type
        self.last_event_position = event.stream_position
        self.version = event.stream_position

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self.context_loaded = True
        self.context_source = event.payload["context_source"]
        self.context_token_count = event.payload["context_token_count"]
        self.model_version = event.payload["model_version"]
        self.established_model_version = event.payload["model_version"]
        self.event_replay_from_position = event.payload.get("event_replay_from_position", 0)
        self.context_loaded_at = event.recorded_at

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        app_id = event.payload["application_id"]
        if app_id not in self.applications_processed:
            self.applications_processed.append(app_id)
        if app_id not in self.decisions_made:
            self.decisions_made.append(app_id)
        self.analyses_completed += 1

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        app_id = event.payload["application_id"]
        if app_id not in self.applications_processed:
            self.applications_processed.append(app_id)
        self.screenings_completed += 1

    def _on_AgentSessionClosed(self, event: StoredEvent) -> None:
        self.is_closed = True
        self.close_reason = event.payload.get("reason", "unknown")

    # -------------------------------------------------------------------------
    # ASSERTION HELPERS
    # -------------------------------------------------------------------------

    def assert_context_loaded(self) -> None:
        """
        Business Rule 2 (Gas Town): Context must be loaded before any decision.
        An agent that has not declared its context cannot make auditable decisions.
        """
        if not self.context_loaded:
            raise DomainError(
                f"AgentSession {self.agent_id}/{self.session_id} has not loaded context. "
                "A AgentContextLoaded event must be the first event in every agent session. "
                "Call start_agent_session before any analysis or decision tool.",
                rule_name="GasTownContextRequirement",
            )

    def assert_model_version_current(self, model_version: str) -> None:
        """
        Business Rule 3 (partial): The model version used in a decision event
        must match the version declared in AgentContextLoaded.
        Prevents an agent from silently upgrading/downgrading its model mid-session.
        """
        if self.established_model_version and model_version != self.established_model_version:
            raise DomainError(
                f"Model version mismatch in session {self.agent_id}/{self.session_id}: "
                f"session established with '{self.established_model_version}', "
                f"decision uses '{model_version}'. "
                "Start a new session to use a different model version.",
                rule_name="ModelVersionLocking",
            )

    def assert_not_closed(self) -> None:
        """Raise DomainError if the session has been explicitly closed."""
        if self.is_closed:
            raise DomainError(
                f"AgentSession {self.agent_id}/{self.session_id} is closed "
                f"(reason: {self.close_reason}). Start a new session.",
                rule_name="ClosedSessionViolation",
            )

    def assert_processed_application(self, application_id: str) -> None:
        """
        Business Rule 6 (partial): This session must have processed the application
        before it can be cited as a contributing session in DecisionGenerated.
        """
        if application_id not in self.decisions_made:
            raise DomainError(
                f"AgentSession {self.agent_id}/{self.session_id} has not made a "
                f"decision for application '{application_id}'. "
                "It cannot be cited as a contributing session in DecisionGenerated.",
                rule_name="CausalChainEnforcement",
            )

    def has_pending_work(self) -> bool:
        """
        Return True if the session's last event suggests incomplete work.
        Used by Gas Town reconciliation to detect NEEDS_RECONCILIATION state.
        A session with a 'Requested' event but no corresponding 'Completed'
        event indicates a partial/interrupted operation.
        """
        partial_indicators = {
            "CreditAnalysisRequested",
            "ComplianceCheckRequested",
        }
        return self.last_event_type in partial_indicators
