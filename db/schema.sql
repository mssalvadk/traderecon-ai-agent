-- ══════════════════════════════════════════════════════════════════════════════
-- TradeRecon AI Agent — SQLite Database Schema
-- ══════════════════════════════════════════════════════════════════════════════
-- Run via: python scripts/setup_db.py
-- ══════════════════════════════════════════════════════════════════════════════

-- ── Pipeline Runs ─────────────────────────────────────────────────────────────
-- Records every pipeline execution — the top-level audit record.
-- Equivalent to a CICS task entry in the CICS monitoring records.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,       -- UUID for this run
    run_date        TEXT NOT NULL,          -- Trade date being reconciled (YYYY-MM-DD)
    started_at      TEXT NOT NULL,          -- ISO 8601 datetime
    completed_at    TEXT,                   -- NULL if still running or failed
    status          TEXT NOT NULL,          -- RUNNING | COMPLETED | FAILED | PARTIAL
    total_breaks    INTEGER DEFAULT 0,
    critical_breaks INTEGER DEFAULT 0,
    high_breaks     INTEGER DEFAULT 0,
    medium_breaks   INTEGER DEFAULT 0,
    source_a_count  INTEGER DEFAULT 0,      -- Trades loaded from source A
    source_b_count  INTEGER DEFAULT 0,      -- Trades loaded from source B
    matched_count   INTEGER DEFAULT 0,      -- Successfully matched trades
    report_path     TEXT,                   -- Path to generated report file
    email_sent      INTEGER DEFAULT 0,      -- 0=false, 1=true
    error_message   TEXT,                   -- Populated if status=FAILED
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ── Breaks ────────────────────────────────────────────────────────────────────
-- Each individual break found during reconciliation.
-- Equivalent to exception records in a traditional recon system.
CREATE TABLE IF NOT EXISTS breaks (
    break_id        TEXT PRIMARY KEY,       -- UUID for this break
    run_id          TEXT NOT NULL,          -- FK → pipeline_runs.run_id
    trade_id        TEXT,                   -- Trade identifier (may be NULL for MISSING)
    break_type      TEXT NOT NULL,          -- MISSING | DUPLICATE | QTY_BREAK | PRICE_BREAK | SETTLE_BREAK | SIDE_BREAK
    severity        TEXT NOT NULL,          -- CRITICAL | HIGH | MEDIUM | LOW
    source_a_value  TEXT,                   -- Value from source A (JSON serialised)
    source_b_value  TEXT,                   -- Value from source B (JSON serialised)
    difference      TEXT,                   -- Calculated difference (if applicable)
    ai_explanation  TEXT,                   -- Claude AI plain-English explanation
    ai_generated_at TEXT,                   -- When the AI explanation was generated
    resolved        INTEGER DEFAULT 0,      -- 0=open, 1=resolved (future feature)
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

-- ── Audit Log ─────────────────────────────────────────────────────────────────
-- Structured audit trail of every agent action.
-- Equivalent to CICS SMF 110 records — every significant event is recorded.
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id        TEXT PRIMARY KEY,       -- UUID for this audit entry
    run_id          TEXT,                   -- FK → pipeline_runs.run_id (NULL for startup events)
    agent_name      TEXT NOT NULL,          -- Which agent generated this entry
    action          TEXT NOT NULL,          -- What action was taken
    status          TEXT NOT NULL,          -- SUCCESS | FAILURE | WARNING | INFO
    detail          TEXT,                   -- JSON detail payload
    duration_ms     INTEGER,                -- How long the action took (milliseconds)
    input_hash      TEXT,                   -- SHA256 hash of input data (for integrity)
    error_message   TEXT,                   -- Populated if status=FAILURE
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

-- ── Indices for query performance ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_breaks_run_id    ON breaks(run_id);
CREATE INDEX IF NOT EXISTS idx_breaks_type      ON breaks(break_type);
CREATE INDEX IF NOT EXISTS idx_breaks_severity  ON breaks(severity);
CREATE INDEX IF NOT EXISTS idx_audit_run_id     ON audit_log(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_agent      ON audit_log(agent_name);
CREATE INDEX IF NOT EXISTS idx_runs_date        ON pipeline_runs(run_date);
CREATE INDEX IF NOT EXISTS idx_runs_status      ON pipeline_runs(status);
