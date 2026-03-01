"""
tests/unit/test_phase0_setup.py
═══════════════════════════════════════════════════════════════════════════════
Phase 0 smoke tests — verify the project scaffold is correctly set up.

These tests don't test business logic yet. They verify:
  - Required files exist
  - Config files are valid YAML
  - Schema file is valid JSON
  - Sample data generator runs without error
  - Database setup runs without error

Run: pytest tests/unit/test_phase0_setup.py -v
═══════════════════════════════════════════════════════════════════════════════
"""

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class TestProjectStructure:
    """Verify all required files and directories exist."""

    def test_readme_exists(self):
        assert (PROJECT_ROOT / "README.md").exists()

    def test_gitignore_exists(self):
        assert (PROJECT_ROOT / ".gitignore").exists()

    def test_env_example_exists(self):
        assert (PROJECT_ROOT / ".env.example").exists()

    def test_requirements_exists(self):
        assert (PROJECT_ROOT / "requirements.txt").exists()

    def test_agents_package_exists(self):
        assert (PROJECT_ROOT / "agents" / "__init__.py").exists()

    def test_tools_package_exists(self):
        assert (PROJECT_ROOT / "tools" / "__init__.py").exists()

    def test_config_dir_exists(self):
        assert (PROJECT_ROOT / "config").is_dir()

    def test_data_samples_dir_exists(self):
        assert (PROJECT_ROOT / "data" / "samples").is_dir()

    def test_db_dir_exists(self):
        assert (PROJECT_ROOT / "db").is_dir()

    def test_scripts_dir_exists(self):
        assert (PROJECT_ROOT / "scripts").is_dir()


class TestConfigFiles:
    """Verify config YAML files are valid and contain required keys."""

    def test_settings_yaml_valid(self):
        path = PROJECT_ROOT / "config" / "settings.yaml"
        assert path.exists(), "settings.yaml not found"
        data = yaml.safe_load(path.read_text())
        assert "pipeline" in data
        assert "scheduler" in data
        assert "data_sources" in data
        assert "database" in data

    def test_tolerance_yaml_valid(self):
        path = PROJECT_ROOT / "config" / "tolerance.yaml"
        assert path.exists(), "tolerance.yaml not found"
        data = yaml.safe_load(path.read_text())
        assert "defaults" in data
        assert "severity" in data
        assert "escalation" in data

    def test_tolerance_has_all_break_types(self):
        path = PROJECT_ROOT / "config" / "tolerance.yaml"
        data = yaml.safe_load(path.read_text())
        severity = data["severity"]
        required_breaks = {"SIDE_BREAK", "MISSING", "DUPLICATE", "QTY_BREAK", "PRICE_BREAK", "SETTLE_BREAK"}
        assert required_breaks.issubset(set(severity.keys()))


class TestSchema:
    """Verify the trade data schema is valid JSON."""

    def test_trade_schema_valid_json(self):
        path = PROJECT_ROOT / "data" / "schemas" / "trade_schema.json"
        assert path.exists(), "trade_schema.json not found"
        data = json.loads(path.read_text())
        assert data["type"] == "object"
        assert "properties" in data
        assert "required" in data

    def test_trade_schema_has_required_fields(self):
        path = PROJECT_ROOT / "data" / "schemas" / "trade_schema.json"
        data = json.loads(path.read_text())
        required = set(data["required"])
        expected = {"trade_id", "trade_date", "settlement_date", "ticker",
                    "isin", "side", "quantity", "price", "consideration"}
        assert expected.issubset(required)


class TestSqliteSchema:
    """Verify SQLite schema SQL file is valid and creates expected tables."""

    def test_schema_sql_exists(self):
        assert (PROJECT_ROOT / "db" / "schema.sql").exists()

    def test_schema_creates_tables(self, tmp_path):
        schema = (PROJECT_ROOT / "db" / "schema.sql").read_text()
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(schema)
        conn.commit()

        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()

        assert "pipeline_runs" in tables
        assert "breaks" in tables
        assert "audit_log" in tables


class TestSampleDataGenerator:
    """Verify the sample data generator runs and produces valid output."""

    def test_generator_runs(self, tmp_path, monkeypatch):
        """Run the generator script and check it exits cleanly."""
        script = PROJECT_ROOT / "scripts" / "generate_sample_data.py"
        result = subprocess.run(
            [sys.executable, str(script), "--num-trades", "20"],
            capture_output=True, text=True,
            cwd=str(PROJECT_ROOT)
        )
        assert result.returncode == 0, f"Generator failed:\n{result.stderr}"

    def test_source_a_created(self):
        path = PROJECT_ROOT / "data" / "samples" / "source_a_trades.csv"
        assert path.exists(), "source_a_trades.csv was not created"
        lines = path.read_text().splitlines()
        assert len(lines) > 1, "source_a_trades.csv is empty"

    def test_source_b_created(self):
        path = PROJECT_ROOT / "data" / "samples" / "source_b_trades.csv"
        assert path.exists(), "source_b_trades.csv was not created"

    def test_reference_data_created(self):
        path = PROJECT_ROOT / "data" / "samples" / "reference_data.csv"
        assert path.exists(), "reference_data.csv was not created"

    def test_source_a_has_correct_headers(self):
        import csv
        path = PROJECT_ROOT / "data" / "samples" / "source_a_trades.csv"
        if not path.exists():
            pytest.skip("Run generate_sample_data.py first")
        with open(path) as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
        required = {"trade_id", "trade_date", "settlement_date", "ticker",
                    "isin", "side", "quantity", "price", "consideration"}
        assert required.issubset(headers)
