"""
src/mcp/server.py
==================
The Ledger MCP Server — entry point.

Exposes The Ledger as enterprise infrastructure via the Model Context Protocol.
AI agents and enterprise systems interact through typed tools (commands) and
resources (queries), never directly with the event store or aggregates.

Architecture:
  Tools    → Commands → Aggregates → EventStore  (write path)
  Resources → Projections                          (read path — no stream replays)

Start with:
    uv run python -m src.mcp.server

Or via fastmcp:
    fastmcp run src/mcp/server.py
"""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from src.startup import initialise

# Wire upcasters before any EventStore use
initialise()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Create the MCP server
mcp = FastMCP(
    name="The Ledger",
    instructions="""
The Ledger is an immutable event store and audit infrastructure for
Apex Financial Services' multi-agent commercial loan processing system.

WRITE PATH (Tools — Commands):
  Use tools to record agent actions and advance application state.
  Every tool call appends immutable events to the event store.
  Tools enforce all business rules — invalid operations are rejected
  with structured errors containing suggested_action for recovery.

  IMPORTANT PRECONDITION: Every agent session MUST call start_agent_session
  before any analysis or decision tool. This is the Gas Town pattern —
  an agent without declared context cannot make auditable decisions.

READ PATH (Resources — Queries):
  Resources read from pre-built projections. They never replay event streams.
  All resources return a data_as_of timestamp indicating projection currency.

ERROR HANDLING:
  All errors return structured objects with:
    error_type: string identifier for programmatic handling
    message: human-readable description
    suggested_action: what to do next (e.g. "reload_stream_and_retry")
  Autonomous recovery is possible for OptimisticConcurrencyError —
  reload the stream and retry with the updated expected_version.
""",
)

# Import and register tools and resources
from src.mcp import tools as _tools  # noqa: E402, F401
from src.mcp import resources as _resources  # noqa: E402, F401


def main() -> None:
    logger.info("Starting The Ledger MCP Server...")
    mcp.run()


if __name__ == "__main__":
    main()
