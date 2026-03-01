# TradeRecon AI Agent — Architecture

## The Core Insight

This system is CICS thinking expressed in Python and Claude API.

Every pattern in this agentic architecture exists because it solves the same
distributed computing problems that CICS solved in 1974 — just with different
technology and a different name on the label.

## CICS → Agentic AI Concept Mapping

| CICS Concept | This Project | Why |
|---|---|---|
| Transaction Manager | OrchestratorAgent | Routes work, never does the work itself |
| Specialist Programs | Each Agent class | Single narrow responsibility |
| Pseudoconversational | Stateless agent calls | Each agent invocation is independent |
| COMMAREA | Pipeline context dict | Explicit state contract between agents |
| TS Queue | SQLite state store | State that survives across agent steps |
| CSD Registry | `tools/` package | Registered capabilities agents can call |
| EXEC CICS LINK | Agent `.run()` call | Synchronous sub-invocation with return |
| HANDLE CONDITION | `try/except` + EIBRESP | Every call checks its response code |
| Syncpoint | Pre-report checkpoint | Commit or rollback before final output |
| RACF / Security | GuardrailAgent | What data is permitted to enter the system |
| SMF 110 Records | AuditAgent + SQLite | Structured telemetry on every action |
| CICS ABEND | Pipeline PARTIAL mode | Failure caught, partial output still sent |

## Agent Design Principles

**1. Every agent is stateless.**
Agents do not store state internally. All state passes through the pipeline
context object or is persisted to SQLite by the AuditAgent.

**2. Every agent validates its inputs.**
GuardrailAgent runs first. Individual agents also validate their own inputs
using pydantic models before processing begins.

**3. Every agent checks its outputs.**
No agent passes unvalidated data to the next. Each output is schema-validated
before being returned.

**4. Failure never silences the pipeline.**
If BreakAnalysisAgent (Claude API) fails, the pipeline continues in PARTIAL
mode — the report is generated without AI explanations, and the email notes
the degraded output. Operations still get a report.

**5. Everything is audited.**
AuditAgent writes a structured JSON entry to SQLite after every agent step.
The audit trail is the production heartbeat.

## Data Flow

```
CSV / Excel / Flat Files
         │
         ▼
 [GuardrailAgent]          ← Validates file existence, schema, hashes inputs
         │
         ▼
 [DataIngestionAgent]      ← Normalises all sources to canonical TradeRecord schema
         │
         ▼
 [ReconciliationAgent]     ← Matches trades, classifies all 6 break types
         │
         ▼
 [BreakAnalysisAgent]      ← Calls Claude API, explains each break in plain English
         │
         ▼
 [ReportGeneratorAgent]    ← Builds Excel report + HTML email summary
         │
         ▼
 [EmailDispatchAgent]      ← Routes to correct recipients by role + severity
         │
         ▼
 [AuditAgent]              ← Writes full audit trail to SQLite
```

## State Store Schema

See `db/schema.sql` for the full SQLite schema.

Three tables:
- `pipeline_runs` — one row per pipeline execution
- `breaks` — one row per break found, with AI explanation
- `audit_log` — one row per agent action (the SMF equivalent)

## Configuration Architecture

No hardcoded values. All behaviour driven by YAML:

- `config/settings.yaml` — schedule, paths, pipeline settings
- `config/tolerance.yaml` — break thresholds, severity rules, escalation
- `config/recipients.yaml` — who gets what report under what conditions

Change behaviour by editing config. Never by changing agent code.

## Security Design

See `docs/SECURITY.md` for the full security design.

Key points:
- API keys in `.env` only — never in config or code
- GuardrailAgent hashes all input files for integrity verification
- Claude API output is schema-validated before use (prompt injection defence)
- All actions written to tamper-evident audit log
- EMAIL_DRY_RUN=true is default for development — no accidental sends
