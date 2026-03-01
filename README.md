# 🤖 TradeRecon AI Agent

> **Production-grade agentic AI system for equities end-of-day trade reconciliation.**
> Built with Python + Anthropic Claude API. From a CICS mainframe engineer learning Agentic AI.

[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Anthropic Claude](https://img.shields.io/badge/AI-Anthropic%20Claude-green.svg)](https://anthropic.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-orange.svg)](https://github.com/astral-sh/ruff)
[![Tests](https://github.com/mssalvadk/traderecon-ai-agent/actions/workflows/tests.yml/badge.svg)](https://github.com/mssalvadk/traderecon-ai-agent/actions/workflows/tests.yml)git
---

## What This Does

TradeRecon AI Agent is a **fully automated, multi-agent AI pipeline** that runs end-of-day and:

1. **Ingests** trade data from multiple sources (CSV, Excel, flat files, SQLite)
2. **Reconciles** trades across sources — identifies all break types
3. **Explains** each break in plain English using Claude AI
4. **Generates** a formatted Excel + HTML report
5. **Emails** the right report to the right recipients based on role and severity
6. **Audits** every action with a structured, tamper-evident log

All without human intervention.

---

## Architecture Overview

```
SCHEDULER TRIGGER
      │
      ▼
┌─────────────────────┐
│  OrchestratorAgent  │  ← Routes, sequences, manages state
└──────────┬──────────┘    (CICS Transaction Manager equivalent)
           │
     ┌─────┼──────────────────────────────┐
     ▼     ▼                              ▼
┌─────────┐ ┌──────────────┐   ┌──────────────────┐
│Guardrail│ │DataIngestion │   │ Reconciliation   │
│  Agent  │ │    Agent     │   │     Agent        │
└─────────┘ └──────────────┘   └──────────────────┘
                                        │
                              ┌─────────┼──────────┐
                              ▼         ▼          ▼
                      ┌────────────┐ ┌──────┐ ┌───────┐
                      │BreakAnalys │ │Report│ │ Email │
                      │   Agent   │ │ Agent│ │ Agent │
                      └────────────┘ └──────┘ └───────┘
                                                   │
                                           ┌───────▼──────┐
                                           │  AuditAgent  │
                                           └──────────────┘
```

### The CICS Connection

This architecture is CICS thinking expressed in Python:

| CICS Concept | This Project |
|---|---|
| Transaction Manager | OrchestratorAgent |
| Specialist Programs | Each specialist Agent |
| TS Queue | SQLite state store |
| CSD Registry | Tool Registry (`tools/`) |
| Syncpoint | Agent checkpoint before report commit |
| SMF 110 Records | AuditAgent structured JSON log |
| HANDLE CONDITION | Every agent validates its inputs and outputs |

---

## Break Types Detected

| Code | Break Type | Severity |
|---|---|---|
| `MISSING` | Trade in one source, absent in other | High |
| `DUPLICATE` | Same Trade ID appears more than once | High |
| `QTY_BREAK` | Quantity / shares mismatch | High |
| `PRICE_BREAK` | Price outside tolerance threshold | Medium |
| `SETTLE_BREAK` | Settlement date mismatch | Medium |
| `SIDE_BREAK` | Buy recorded as Sell or vice versa | Critical |

---

## Quickstart

### 1. Clone & Setup

```bash
git clone https://github.com/YOUR_USERNAME/traderecon-ai-agent.git
cd traderecon-ai-agent

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy the environment template
copy .env.example .env

# Edit .env and add your Anthropic API key
# ANTHROPIC_API_KEY=your_key_here
```

### 3. Generate Sample Data & Initialise DB

```bash
python scripts/generate_sample_data.py
python scripts/setup_db.py
```

### 4. Run the Pipeline

```bash
# One-shot manual run
python scripts/run_pipeline.py

# Or let the scheduler run it automatically at 18:00 UTC
python main.py
```

---

## Project Structure

```
traderecon-ai-agent/
├── agents/              # All agent classes
├── tools/               # Reusable tool modules
├── config/              # YAML configuration files
├── data/samples/        # Sample trade CSV files
├── prompts/             # Claude AI prompt templates
├── reports/templates/   # Excel + HTML report templates
├── db/                  # SQLite schema
├── tests/               # Unit + integration tests
├── docs/                # Architecture docs
└── scripts/             # Setup and run scripts
```

---

## Configuration

All behaviour is driven by YAML config — no hardcoded values:

- `config/settings.yaml` — schedule, file paths, pipeline settings
- `config/tolerance.yaml` — break tolerance thresholds per asset class
- `config/recipients.yaml` — role-based email routing
- `config/logging.yaml` — structured logging configuration

---

## Running Tests

```bash
pytest tests/ -v --cov=agents --cov=tools --cov-report=term-missing
```

---

## Learning Journey

This project is part of a structured **Agentic AI Development & Security** learning programme. Each phase of development is documented in the commit history following [Conventional Commits](https://www.conventionalcommits.org/) standard.

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for deep-dive design notes including the full CICS-to-Agentic AI concept mapping.

---

## Author

Mainframe CICS Engineer | Banking & Financial Services | Learning Agentic AI Architecture

*"The patterns in agentic AI are not new. They are CICS thinking, expressed in Python and Claude API."*

---

## License

MIT — see [LICENSE](LICENSE)
