# Changelog

All notable changes to TradeRecon AI Agent are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

---

## [Unreleased]

### Phase 0 — Repository Bootstrap (Current)
- Repository scaffold with professional folder structure
- Configuration files: settings, tolerance, recipients, logging
- Environment template (.env.example)
- SQLite schema (pipeline_runs, breaks, audit_log)
- Sample trade data generator with all 6 break types
- Database initialisation script
- README, ARCHITECTURE, SECURITY documentation

---

## Roadmap

| Version | Phase | Description |
|---|---|---|
| 0.1.0 | Phase 0 | Repository bootstrap & professional setup |
| 0.2.0 | Phase 1 | DataIngestionAgent + GuardrailAgent + AuditAgent |
| 0.3.0 | Phase 2 | ReconciliationAgent — all 6 break types |
| 0.4.0 | Phase 3 | BreakAnalysisAgent — Claude AI explanations |
| 0.5.0 | Phase 4 | ReportGeneratorAgent + EmailDispatchAgent |
| 1.0.0 | Phase 5–6 | OrchestratorAgent, scheduling, full hardening |
