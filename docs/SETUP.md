# Setup Guide — Windows 10/11 + VS Code

## Prerequisites

- Python 3.10+ installed
- Git installed
- VS Code installed
- GitHub account

---

## Step 1: Clone the Repository

Open **Git Bash** or **VS Code Terminal** and run:

```bash
git clone https://github.com/YOUR_USERNAME/traderecon-ai-agent.git
cd traderecon-ai-agent
```

---

## Step 2: Create a Virtual Environment

Always use a virtual environment — keeps your project dependencies isolated.

```bash
# Create the venv
python -m venv venv

# Activate it (Windows CMD)
venv\Scripts\activate

# Activate it (Windows PowerShell)
venv\Scripts\Activate.ps1

# Activate it (Git Bash)
source venv/Scripts/activate

# You should see (venv) at the start of your prompt
```

---

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

This installs everything: anthropic, pandas, openpyxl, pydantic, APScheduler, jinja2, pytest, ruff.

---

## Step 4: Configure Environment Variables

```bash
# Copy the template
copy .env.example .env
```

Open `.env` in VS Code and fill in:

```
ANTHROPIC_API_KEY=your_key_here
EMAIL_SENDER=your_email@gmail.com
EMAIL_APP_PASSWORD=your_gmail_app_password
EMAIL_DRY_RUN=true
ENVIRONMENT=development
DEBUG=true
```

**Get your Anthropic API key:** https://console.anthropic.com

**Get a Gmail App Password:** https://support.google.com/accounts/answer/185833

---

## Step 5: Generate Sample Data

```bash
python scripts/generate_sample_data.py
```

This creates:
- `data/samples/source_a_trades.csv` — internal blotter (50 trades)
- `data/samples/source_b_trades.csv` — counterparty feed (with breaks)
- `data/samples/reference_data.csv` — security master

---

## Step 6: Initialise the Database

```bash
python scripts/setup_db.py
```

Creates `db/traderecon.db` with the full schema.

---

## Step 7: Verify Everything Works

```bash
# Run the test suite
pytest tests/ -v

# Run the manual pipeline trigger (scaffold only in Phase 0)
python scripts/run_pipeline.py
```

---

## VS Code Recommended Extensions

Install these for the best development experience:

- **Python** (ms-python.python)
- **Pylance** (ms-python.vscode-pylance)
- **Ruff** (charliermarsh.ruff)
- **GitLens** (eamodio.gitlens)
- **SQLite Viewer** (qwtel.sqlite-viewer)
- **YAML** (redhat.vscode-yaml)
- **Even Better TOML** (tamasfe.even-better-toml)

---

## Troubleshooting

**`venv\Scripts\activate` fails in PowerShell:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**`pip install` is slow:**
```bash
pip install -r requirements.txt --index-url https://pypi.org/simple
```

**`python` not found:**
Make sure Python is on your PATH. Reinstall Python and check "Add to PATH" during setup.
