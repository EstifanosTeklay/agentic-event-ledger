"""
src/integrity/audit_chain.py
=============================
Cryptographic audit chain for the AuditLedger aggregate.

Each AuditIntegrityCheckRun event records:
  integrity_hash = sha256(previous_hash + concat(event_hashes))

This forms a blockchain-style chain. Any post-hoc modification of
stored events breaks the chain — detectable on the next integrity check.

Used in:
  - Step 1 of video demo (complete decision history with integrity verification)
  - generate_regulatory_package() (Phase 6)
  - run_integrity_check MCP tool (Phase 5)
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.event_store import EventStore

from src.models.events import (
    AuditIntegrityCheckRun,
    IntegrityCheckResult,
    StoredEvent,
)

logger = logging.getLogger(__name__)

# Genesis hash — the starting point for the chain (no previous check)
GENESIS_HASH = "0" * 64


@dataclass
class IntegrityCheckResult:
    """Result of a cryptographic integrity check."""
    entity_id:             str
    entity_type:           str
    events_verified:       int
    chain_valid:           bool
    tamper_detected:       bool
    integrity_hash:        str
    previous_hash:         str
    check_timestamp:       datetime
    failed_at_position:    int | None = None
    failure_detail:        str | None = None


def _hash_event(event: StoredEvent) -> str:
    """
    Compute a deterministic SHA-256 hash of an event's payload.
    Uses sorted JSON serialisation for determinism across Python versions.
    """
    canonical = json.dumps(
        {
            "event_id":       str(event.event_id),
            "stream_id":      event.stream_id,
            "stream_position": event.stream_position,
            "event_type":     event.event_type,
            "event_version":  event.event_version,
            "payload":        event.payload,
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _compute_chain_hash(previous_hash: str, event_hashes: list[str]) -> str:
    """
    Compute the chain hash:
      sha256(previous_hash + event_hash_1 + event_hash_2 + ...)
    """
    combined = previous_hash + "".join(event_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


async def run_integrity_check(
    store: "EventStore",
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Run a cryptographic integrity check on an entity's audit chain.

    Steps:
    1. Load all events for the entity's primary stream
    2. Load the last AuditIntegrityCheckRun event (if any)
    3. Hash all events since the last check
    4. Verify: new_hash = sha256(previous_hash + event_hashes)
    5. Append new AuditIntegrityCheckRun event to audit stream
    6. Return result with chain_valid and tamper_detected flags

    Args:
        store:       EventStore instance
        entity_type: e.g. "LoanApplication", "AgentSession"
        entity_id:   e.g. "APEX-001"

    Returns:
        IntegrityCheckResult with full chain verification details
    """
    from src.models.events import StreamNotFoundError

    primary_stream = _primary_stream_id(entity_type, entity_id)
    audit_stream   = f"audit-{entity_type}-{entity_id}"

    # ── Step 1: Load primary stream events ───────────────────────────────────
    try:
        primary_events = await store.load_stream(primary_stream)
    except StreamNotFoundError:
        primary_events = []

    # ── Step 2: Load audit stream to get last integrity check ────────────────
    previous_hash = GENESIS_HASH
    last_check_position = 0

    try:
        audit_events = await store.load_stream(audit_stream)
        # Find the most recent AuditIntegrityCheckRun
        for event in reversed(audit_events):
            if event.event_type == "AuditIntegrityCheckRun":
                previous_hash = event.payload.get("integrity_hash", GENESIS_HASH)
                last_check_position = event.payload.get(
                    "events_verified_count", 0
                )
                break
    except StreamNotFoundError:
        pass  # No prior checks — start from genesis

    # ── Step 3: Hash all events since last check ─────────────────────────────
    events_to_check = primary_events[last_check_position:]
    event_hashes = [_hash_event(e) for e in events_to_check]

    # ── Step 4: Compute and verify chain hash ─────────────────────────────────
    new_hash = _compute_chain_hash(previous_hash, event_hashes)

    # Tamper detection: recompute hashes of already-checked events and verify
    # they match what was recorded in the previous AuditIntegrityCheckRun
    chain_valid = True
    tamper_detected = False
    failed_at = None
    failure_detail = None

    if last_check_position > 0 and audit_events:
        # Find previous check event
        prev_check_events = [
            e for e in audit_events
            if e.event_type == "AuditIntegrityCheckRun"
        ]
        if prev_check_events:
            prev_check = prev_check_events[-1]
            recorded_hash = prev_check.payload.get("integrity_hash")
            # Re-verify the events covered by the previous check
            prev_events = primary_events[:last_check_position]
            prev_hashes = [_hash_event(e) for e in prev_events]
            recomputed = _compute_chain_hash(
                prev_check.payload.get("previous_hash", GENESIS_HASH),
                prev_hashes,
            )
            if recomputed != recorded_hash:
                chain_valid = False
                tamper_detected = True
                failed_at = last_check_position
                failure_detail = (
                    f"Chain broken: recomputed hash {recomputed[:16]}... "
                    f"does not match recorded {recorded_hash[:16]}..."
                )
                logger.critical(
                    "TAMPER DETECTED in stream %s at position %d",
                    primary_stream, last_check_position,
                )

    # ── Step 5: Append AuditIntegrityCheckRun to audit stream ────────────────
    check_timestamp = datetime.now(timezone.utc)
    integrity_event = AuditIntegrityCheckRun(
        entity_id=entity_id,
        entity_type=entity_type,
        check_timestamp=check_timestamp,
        events_verified_count=len(primary_events),
        integrity_hash=new_hash,
        previous_hash=previous_hash,
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
    )

    try:
        # Try to append to existing audit stream
        audit_version = await store.stream_version(audit_stream)
        await store.append(
            stream_id=audit_stream,
            events=[integrity_event],
            expected_version=audit_version,
        )
    except StreamNotFoundError:
        # First integrity check — create the audit stream
        await store.append(
            stream_id=audit_stream,
            events=[integrity_event],
            expected_version=-1,
        )

    logger.info(
        "Integrity check: %s/%s — %d events verified, chain_valid=%s, tamper=%s",
        entity_type, entity_id, len(primary_events),
        chain_valid, tamper_detected,
    )

    # ── Step 6: Return result ─────────────────────────────────────────────────
    return IntegrityCheckResult(
        entity_id=entity_id,
        entity_type=entity_type,
        events_verified=len(primary_events),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=new_hash,
        previous_hash=previous_hash,
        check_timestamp=check_timestamp,
        failed_at_position=failed_at,
        failure_detail=failure_detail,
    )


async def verify_chain_integrity(
    store: "EventStore",
    entity_type: str,
    entity_id: str,
) -> bool:
    """
    Quick integrity check — returns True if chain is valid, False if tampered.
    Does NOT write a new AuditIntegrityCheckRun event.
    Used for read-only verification (e.g. regulatory package generation).
    """
    from src.models.events import StreamNotFoundError

    audit_stream = f"audit-{entity_type}-{entity_id}"
    primary_stream = _primary_stream_id(entity_type, entity_id)

    try:
        audit_events = await store.load_stream(audit_stream)
        primary_events = await store.load_stream(primary_stream)
    except StreamNotFoundError:
        return True  # No checks run yet — chain is vacuously valid

    check_events = [
        e for e in audit_events
        if e.event_type == "AuditIntegrityCheckRun"
    ]
    if not check_events:
        return True

    # Verify the most recent check
    last_check = check_events[-1]
    events_verified = last_check.payload.get("events_verified_count", 0)
    recorded_hash   = last_check.payload.get("integrity_hash")
    prev_hash       = last_check.payload.get("previous_hash", GENESIS_HASH)

    events_covered = primary_events[:events_verified]
    hashes = [_hash_event(e) for e in events_covered]
    recomputed = _compute_chain_hash(prev_hash, hashes)

    return recomputed == recorded_hash


def _primary_stream_id(entity_type: str, entity_id: str) -> str:
    """Map entity type to its primary stream ID format."""
    mapping = {
        "LoanApplication": f"loan-{entity_id}",
        "AgentSession":    f"agent-{entity_id}",
        "ComplianceRecord": f"compliance-{entity_id}",
    }
    return mapping.get(entity_type, f"{entity_type.lower()}-{entity_id}")
