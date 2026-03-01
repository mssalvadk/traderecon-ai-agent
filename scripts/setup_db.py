"""
scripts/setup_db.py
═══════════════════════════════════════════════════════════════════════════════
Initialises the SQLite database from db/schema.sql.

Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
Wipes existing data if --reset flag is passed.

Usage:
    python scripts/setup_db.py
    python scripts/setup_db.py --reset    # drops and recreates all tables
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import sqlite3
from pathlib import Path


DB_PATH     = Path("db/traderecon.db")
SCHEMA_PATH = Path("db/schema.sql")


def setup(reset: bool = False) -> None:
    """Initialise the database from schema.sql."""

    # Ensure db directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not SCHEMA_PATH.exists():
        print(f"ERROR: Schema file not found at {SCHEMA_PATH}")
        raise SystemExit(1)

    schema = SCHEMA_PATH.read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if reset:
        print("⚠  Resetting database — dropping all tables...")
        cursor.executescript("""
            DROP TABLE IF EXISTS audit_log;
            DROP TABLE IF EXISTS breaks;
            DROP TABLE IF EXISTS pipeline_runs;
        """)
        print("  ✓ Tables dropped")

    cursor.executescript(schema)
    conn.commit()

    # Verify tables created
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]

    conn.close()

    print()
    print("TradeRecon AI Agent — Database Setup")
    print("=" * 60)
    print(f"  Database : {DB_PATH.resolve()}")
    print(f"  Schema   : {SCHEMA_PATH.resolve()}")
    print(f"  Tables   : {', '.join(tables)}")
    print()
    print("  ✓ Database ready")
    print()
    print("  Next step:")
    print("    python scripts/run_pipeline.py")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialise the TradeRecon SQLite database")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate all tables (WARNING: deletes all data)",
    )
    args = parser.parse_args()
    setup(reset=args.reset)
