"""
scripts/demo_upcasting.py
==========================
Video Demo — Step 4: Upcasting & Immutability

Shows in real time:
  - A v1 event stored directly in the database
  - Loading it via EventStore → arrives as v2 (upcasted)
  - Querying the raw DB row → payload is UNCHANGED (still v1)
  - confidence_score=None (no fabrication)
  - model_version inferred with _inferred suffix

Run with:
    uv run python scripts/demo_upcasting.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.startup import initialise
initialise()

from src.event_store import EventStore

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger")

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def green(t):  return f"{GREEN}{t}{RESET}"
def red(t):    return f"{RED}{t}{RESET}"
def yellow(t): return f"{YELLOW}{t}{RESET}"
def cyan(t):   return f"{CYAN}{t}{RESET}"
def bold(t):   return f"{BOLD}{t}{RESET}"

DIV  = "─" * 64
DIV2 = "═" * 64


def print_payload(label: str, version: int, payload: dict, colour_fn) -> None:
    print(f"\n  {bold(label)}")
    print(f"  {'─'*50}")
    print(f"  event_version : {colour_fn(str(version))}")
    for k, v in payload.items():
        if k in ("model_version", "confidence_score", "regulatory_basis"):
            print(f"  {cyan(k):<30} {colour_fn(repr(v))}")
        else:
            print(f"  {k:<30} {repr(v)}")
    print(f"  {'─'*50}")


async def main() -> None:
    print(f"\n{DIV2}")
    print(f"{bold('  THE LEDGER — Upcasting & Immutability Demo')}")
    print(f"  Step 4 of Video Demo · Apex Financial Services")
    print(f"{DIV2}")

    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "TRUNCATE TABLE events, event_streams, outbox RESTART IDENTITY CASCADE"
    )
    await conn.close()

    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=5)
    store = EventStore(pool=pool)

    stream_id = "loan-UPCAST-DEMO-001"

    # ── Step 1: Insert a genuine v1 event directly into the database ──────────
    print(f"\n{DIV}")
    print(f"{bold('STEP 1')}  Inserting a genuine v1 event directly into PostgreSQL")
    print(f"         (simulating a historical 2024 event before v2 was introduced)")
    print(DIV)

    v1_payload = {
        "application_id":      "APEX-DEMO-001",
        "agent_id":            "agent-credit-001",
        "session_id":          "sess-2024-001",
        "risk_tier":           "MEDIUM",
        "recommended_limit_usd": "750000.00",
        "analysis_duration_ms": 1340,
        "input_data_hash":     "sha256-2024-legacy-input",
        # NO model_version, confidence_score, or regulatory_basis
        # This is a genuine v1 schema from 2024
    }

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, 'LoanApplication', 0)
            ON CONFLICT DO NOTHING
            """,
            stream_id,
        )
        await conn.execute(
            """
            INSERT INTO events
                (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 1, 'CreditAnalysisCompleted', 1, $2::jsonb, '{}'::jsonb)
            """,
            stream_id,
            json.dumps(v1_payload),
        )
        await conn.execute(
            "UPDATE event_streams SET current_version = 1 WHERE stream_id = $1",
            stream_id,
        )

    print(f"  {green('✓')} v1 event inserted with event_version=1")
    print(f"  {yellow('⚠')} Missing fields: model_version, confidence_score, regulatory_basis")

    # ── Step 2: Query raw DB row ───────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('STEP 2')}  Querying raw PostgreSQL row (what is actually stored)")
    print(DIV)

    async with pool.acquire() as conn:
        raw = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1",
            stream_id,
        )

    raw_payload = raw["payload"]
    if isinstance(raw_payload, str):
        raw_payload = json.loads(raw_payload)
    raw_payload = dict(raw_payload)

    print_payload("RAW DB PAYLOAD (what PostgreSQL stores)", raw["event_version"], raw_payload, yellow)

    # ── Step 3: Load via EventStore (upcasting applied) ────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('STEP 3')}  Loading via EventStore.load_stream() — upcasting applied")
    print(DIV)

    loaded_events = await store.load_stream(stream_id)
    loaded = loaded_events[0]

    print_payload("LOADED EVENT PAYLOAD (after upcasting)", loaded.event_version, loaded.payload, green)

    # ── Step 4: Verify DB is unchanged ───────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('STEP 4')}  Re-querying raw DB row — must be UNCHANGED")
    print(DIV)

    async with pool.acquire() as conn:
        raw_after = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1",
            stream_id,
        )

    raw_payload_after = raw_after["payload"]
    if isinstance(raw_payload_after, str):
        raw_payload_after = json.loads(raw_payload_after)
    raw_payload_after = dict(raw_payload_after)

    unchanged = raw_payload == raw_payload_after and raw_after["event_version"] == 1
    status = green("UNCHANGED ✓") if unchanged else red("MUTATED ✗ — BUG!")
    print(f"  DB event_version : {raw_after['event_version']} (still v1) → {status}")
    print(f"  DB payload keys  : {list(raw_payload_after.keys())}")
    print(f"  model_version present in DB : {'model_version' in raw_payload_after}")

    # ── Step 5: Key observations ──────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('OBSERVATIONS')}")
    print(DIV)

    print(f"\n  {cyan('confidence_score')} = {repr(loaded.payload.get('confidence_score'))}")
    print(f"  → {green('None (correct)')} — this score was never computed in 2024.")
    print(f"    Fabricating 0.5 would corrupt AgentPerformanceLedger analytics.")
    print(f"    null signals absence; fabrication signals false precision.")

    print(f"\n  {cyan('model_version')} = {repr(loaded.payload.get('model_version'))}")
    print(f"  → {yellow('Inferred')} from deployment timeline against recorded_at date.")
    print(f"    '_inferred' suffix flags this as approximate, not verified.")

    print(f"\n  {cyan('regulatory_basis')} = {repr(loaded.payload.get('regulatory_basis'))}")
    print(f"  → {green('Deterministic lookup')} from regulation timeline. ~0% error rate.")

    # ── Assertions ────────────────────────────────────────────────────────────
    assert loaded.event_version == 2, "Loaded event must be v2"
    assert raw_after["event_version"] == 1, "Stored event must remain v1"
    assert loaded.payload.get("confidence_score") is None, "Must not fabricate confidence"
    assert "_inferred" in loaded.payload.get("model_version", ""), "Must flag as inferred"
    assert raw_payload == raw_payload_after, "DB payload must be unchanged"

    print(f"\n{DIV2}")
    print(f"{bold('  UPCASTING & IMMUTABILITY DEMO COMPLETE — Step 4 verified')}")
    print(f"  Stored: v1 (unchanged) | Loaded: v2 (upcasted) | DB: immutable")
    print(f"{DIV2}\n")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
