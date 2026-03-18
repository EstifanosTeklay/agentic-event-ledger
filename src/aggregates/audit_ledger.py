"""
src/aggregates/audit_ledger.py
================================
AuditLedgerAggregate — cross-cutting append-only audit trail.

This aggregate enforces:
  - Immutability: once appended, no event can be logically removed
  - Cross-stream causal ordering via correlation_id chains
  - Cryptographic integrity via hash chain (Phase 4)

One AuditLedger stream exists per business entity, e.g.:
  audit-LoanApplication-ABC-001
  audit-AgentSession-agent-x-session-y

The AuditLedger is NOT a projection of other streams.
It is a first-class aggregate that receives its own events.
The integrity check writes AuditIntegrityCheckRun events to this stream.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.models.events import (
    AuditIntegrityCheckRun,
    DomainError,
    StoredEvent,
)

if TYPE_CHECKING:
    from src.event_store import EventStore

logger = logging.getLogger(__name__)


class AuditLedgerAggregate:
    """
    Append-only audit trail for a specific business entity.
    Stream ID: audit-{entity_type}-{entity_id}
    """

    def __init__(self, entity_type: str, entity_id: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.version: int = 0

        # Integrity chain state
        self.last_integrity_hash: str = "0" * 64  # genesis hash
        self.last_integrity_check_position: int = 0
        self.integrity_check_count: int = 0
        self.tamper_detected: bool = False

        # Causal ordering
        self.known_correlation_ids: set[str] = set()
        self.event_count: int = 0

    # -------------------------------------------------------------------------
    # FACTORY
    # -------------------------------------------------------------------------

    @classmethod
    async def load(
        cls,
        store: "EventStore",
        entity_type: str,
        entity_id: str,
    ) -> "AuditLedgerAggregate":
        stream_id = f"audit-{entity_type}-{entity_id}"
        events = await store.load_stream(stream_id)
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        for event in events:
            agg._apply(event)
        return agg

    @classmethod
    def stream_id_for(cls, entity_type: str, entity_id: str) -> str:
        return f"audit-{entity_type}-{entity_id}"

    # -------------------------------------------------------------------------
    # EVENT APPLICATION
    # -------------------------------------------------------------------------

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)
        self.event_count += 1
        self.version = event.stream_position

        # Track correlation IDs for causal ordering verification
        correlation_id = event.metadata.get("correlation_id")
        if correlation_id:
            self.known_correlation_ids.add(correlation_id)

    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        self.last_integrity_hash = event.payload["integrity_hash"]
        self.last_integrity_check_position = event.stream_position
        self.integrity_check_count += 1
        if event.payload.get("tamper_detected"):
            self.tamper_detected = True
            logger.critical(
                "TAMPER DETECTED in audit stream audit-%s-%s at position %d",
                self.entity_type,
                self.entity_id,
                event.stream_position,
            )

    # -------------------------------------------------------------------------
    # ASSERTION HELPERS
    # -------------------------------------------------------------------------

    def assert_no_tamper_detected(self) -> None:
        """Raise DomainError if a previous integrity check detected tampering."""
        if self.tamper_detected:
            raise DomainError(
                f"Audit ledger for {self.entity_type}/{self.entity_id} has a "
                "previously detected tamper event. This audit trail may not be "
                "relied upon for regulatory purposes until investigated.",
                rule_name="TamperDetected",
            )
