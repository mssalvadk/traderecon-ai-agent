"""
tools/state_store.py
===============================================================================
SQLite persistence layer for pipeline state and audit trail.

This is the TS Queue equivalent in CICS terms -- state that must survive
across agent boundaries is written here. Every agent reads from and writes
to this store via the PipelineContext, never via internal memory.

All writes are explicit. Nothing is assumed to persist unless written.
===============================================================================
"""

import json
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from tools.schemas import AuditEntry, AuditStatus, BreakRecord, PipelineRun, PipelineStatus


# ── Helpers ───────────────────────────────────────────────────────────────────

def new_id() -> str:
    """Generate a new UUID string."""
    return str(uuid.uuid4())


def to_iso(dt: Optional[datetime | date]) -> Optional[str]:
    """Convert datetime/date to ISO 8601 string for SQLite storage."""
    if dt is None:
        return None
    return dt.isoformat()


def from_iso_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 string to datetime."""
    if not s:
        return None
    return datetime.fromisoformat(s)


# ── State Store ───────────────────────────────────────────────────────────────

class StateStore:
    """
    SQLite-backed state and audit persistence for the pipeline.

    Usage:
        store = StateStore("db/traderecon.db")
        store.create_run(run)
        store.write_audit(entry)
        store.write_breaks(breaks)
        store.complete_run(run_id, status)
    """

    def __init__(self, db_path: str | Path = "db/traderecon.db"):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Database not found at {self.db_path}. "
                f"Run: python scripts/setup_db.py"
            )

    def _connect(self) -> sqlite3.Connection:
        """Open a database connection with sensible defaults."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")   # Better concurrent access
        conn.execute("PRAGMA foreign_keys=ON")    # Enforce FK constraints
        return conn

    # ── Pipeline Runs ──────────────────────────────────────────────────────

    def create_run(self, run: PipelineRun) -> None:
        """Insert a new pipeline run record. Called at pipeline start."""
        sql = """
            INSERT INTO pipeline_runs (
                run_id, run_date, started_at, status,
                source_a_count, source_b_count
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, (
                run.run_id,
                to_iso(run.run_date),
                to_iso(run.started_at),
                run.status,
                run.source_a_count,
                run.source_b_count,
            ))

    def complete_run(
        self,
        run_id: str,
        status: PipelineStatus | str,
        total_breaks: int = 0,
        critical_breaks: int = 0,
        high_breaks: int = 0,
        medium_breaks: int = 0,
        source_a_count: int = 0,
        source_b_count: int = 0,
        matched_count: int = 0,
        report_path: Optional[str] = None,
        email_sent: bool = False,
        error_message: Optional[str] = None,
    ) -> None:
        """Update a pipeline run to its final state. Called at pipeline end."""
        sql = """
            UPDATE pipeline_runs SET
                completed_at    = ?,
                status          = ?,
                total_breaks    = ?,
                critical_breaks = ?,
                high_breaks     = ?,
                medium_breaks   = ?,
                source_a_count  = ?,
                source_b_count  = ?,
                matched_count   = ?,
                report_path     = ?,
                email_sent      = ?,
                error_message   = ?
            WHERE run_id = ?
        """
        with self._connect() as conn:
            conn.execute(sql, (
                to_iso(datetime.now(timezone.utc)),
                str(status),
                total_breaks,
                critical_breaks,
                high_breaks,
                medium_breaks,
                source_a_count,
                source_b_count,
                matched_count,
                report_path,
                1 if email_sent else 0,
                error_message,
                run_id,
            ))

    def get_run(self, run_id: str) -> Optional[dict]:
        """Retrieve a pipeline run by ID."""
        sql = "SELECT * FROM pipeline_runs WHERE run_id = ?"
        with self._connect() as conn:
            row = conn.execute(sql, (run_id,)).fetchone()
            return dict(row) if row else None

    # ── Breaks ─────────────────────────────────────────────────────────────

    def write_breaks(self, breaks: list[BreakRecord]) -> None:
        """
        Bulk insert break records. Called after ReconciliationAgent completes.
        Uses INSERT OR IGNORE to be safe on re-runs.
        """
        if not breaks:
            return

        sql = """
            INSERT OR IGNORE INTO breaks (
                break_id, run_id, trade_id, break_type, severity,
                source_a_value, source_b_value, difference,
                ai_explanation, ai_generated_at, resolved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                b.break_id,
                b.run_id,
                b.trade_id,
                b.break_type,
                b.severity,
                b.source_a_value,
                b.source_b_value,
                b.difference,
                b.ai_explanation,
                to_iso(b.ai_generated_at),
                1 if b.resolved else 0,
            )
            for b in breaks
        ]
        with self._connect() as conn:
            conn.executemany(sql, rows)

    def update_break_explanation(
        self,
        break_id: str,
        explanation: str,
    ) -> None:
        """Update a break record with its AI-generated explanation."""
        sql = """
            UPDATE breaks SET
                ai_explanation  = ?,
                ai_generated_at = ?
            WHERE break_id = ?
        """
        with self._connect() as conn:
            conn.execute(sql, (explanation, to_iso(datetime.timezone.utc), break_id))

    def get_breaks_for_run(self, run_id: str) -> list[dict]:
        """Retrieve all breaks for a pipeline run."""
        sql = "SELECT * FROM breaks WHERE run_id = ? ORDER BY severity, break_type"
        with self._connect() as conn:
            rows = conn.execute(sql, (run_id,)).fetchall()
            return [dict(row) for row in rows]

    # ── Audit Log ──────────────────────────────────────────────────────────

    def write_audit(self, entry: AuditEntry) -> None:
        """
        Write a single audit log entry.

        This is the SMF 110 equivalent -- called after every significant
        agent action regardless of success or failure.
        """
        sql = """
            INSERT INTO audit_log (
                audit_id, run_id, agent_name, action, status,
                detail, duration_ms, input_hash, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, (
                entry.audit_id,
                entry.run_id,
                entry.agent_name,
                entry.action,
                entry.status,
                json.dumps(entry.detail) if entry.detail else None,
                entry.duration_ms,
                entry.input_hash,
                entry.error_message,
                to_iso(entry.created_at),
            ))

    def get_audit_for_run(self, run_id: str) -> list[dict]:
        """Retrieve all audit entries for a pipeline run, ordered by time."""
        sql = """
            SELECT * FROM audit_log
            WHERE run_id = ?
            ORDER BY created_at ASC
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (run_id,)).fetchall()
            return [dict(row) for row in rows]

    # ── Convenience ────────────────────────────────────────────────────────

    def quick_audit(
        self,
        agent_name: str,
        action: str,
        status: AuditStatus | str,
        run_id: Optional[str] = None,
        detail: Optional[dict] = None,
        duration_ms: Optional[int] = None,
        input_hash: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Convenience method to write an audit entry without constructing
        a full AuditEntry object. Used throughout agent code for brevity.
        """
        entry = AuditEntry(
            audit_id=new_id(),
            run_id=run_id,
            agent_name=agent_name,
            action=action,
            status=status.value if hasattr(status, "value") else str(status),
            detail=detail,
            duration_ms=duration_ms,
            input_hash=input_hash,
            error_message=error_message,
        )
        self.write_audit(entry)
