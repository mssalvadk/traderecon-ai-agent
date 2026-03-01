# Security Design — TradeRecon AI Agent

## Principles

This system handles sensitive financial trade data. Security is not an afterthought — it is a first-class design requirement in every agent.

---

## Secret Management

**Rule: Secrets never appear in code or config files. Ever.**

- All API keys and credentials live in `.env` only
- `.env` is in `.gitignore` — it can never be committed
- `.env.example` shows the required keys without values
- The system fails fast on startup if required env vars are missing

---

## Input Validation (GuardrailAgent)

Every data file entering the pipeline is validated before any processing:

- **File existence check** — fail fast if source file is missing
- **Schema validation** — pydantic models enforce the trade data contract
- **File integrity hash** — SHA256 hash of each source file written to audit log
- **Size sanity check** — reject suspiciously empty or enormous files
- **Date validation** — reject trades with dates outside expected range

---

## AI Output Validation

Claude API responses are treated as untrusted input — they are validated before use:

- All Claude responses are parsed against a pydantic schema
- If the response fails schema validation, the break is flagged but the pipeline continues
- This defends against prompt injection attempts via trade data fields
- Token limits are enforced — no runaway API calls

---

## Audit Trail

Every agent action is written to the SQLite audit log:

- Agent name, action, status, timestamp
- Duration in milliseconds
- SHA256 hash of input data
- Error messages for failures

The audit log is append-only in normal operation — no updates, no deletes.

---

## Email Security

- Default mode is `EMAIL_DRY_RUN=true` — no emails sent during development
- Gmail App Password used (never the real Gmail password)
- Recipient list is config-driven — no email addresses in code
- In development mode, all emails route to `dev_override` recipient only

---

## Data Handling

- Real trade data never committed to Git
- `data/samples/` contains only generated synthetic data
- `reports/output/` is `.gitignored`
- SQLite database file is `.gitignored`
- Logs directory is `.gitignored`
