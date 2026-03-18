#!/usr/bin/env python3
"""
scripts/run_migrations.py
=========================
Applies schema.sql to the target PostgreSQL database.
Safe to run multiple times — all DDL uses IF NOT EXISTS.

Usage:
    python scripts/run_migrations.py
    python scripts/run_migrations.py --url postgresql://user:pass@host/db
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

load_dotenv()


async def run_migrations(database_url: str) -> None:
    schema_path = Path(__file__).parent.parent / "src" / "schema.sql"
    if not schema_path.exists():
        print(f"ERROR: schema.sql not found at {schema_path}", file=sys.stderr)
        sys.exit(1)

    sql = schema_path.read_text()

    print(f"Connecting to database...")
    conn = await asyncpg.connect(dsn=database_url)

    try:
        print("Applying schema.sql...")
        await conn.execute(sql)
        print("✓ Migrations applied successfully.")

        # Verify key tables exist
        tables = await conn.fetch(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
            """
        )
        table_names = [r["tablename"] for r in tables]
        required = [
            "events", "event_streams", "projection_checkpoints",
            "outbox", "application_summary", "agent_performance_ledger",
            "compliance_audit_view", "compliance_audit_snapshots",
        ]
        for t in required:
            status = "✓" if t in table_names else "✗ MISSING"
            print(f"  {status}  {t}")

    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Ledger database migrations")
    parser.add_argument(
        "--url",
        default=os.getenv("DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger"),
        help="PostgreSQL connection URL",
    )
    args = parser.parse_args()
    asyncio.run(run_migrations(args.url))


if __name__ == "__main__":
    main()
