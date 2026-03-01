"""
tests/unit/test_phase1_ingestion.py
===============================================================================
Phase 1 unit tests -- data ingestion, schema validation, and state store.

Tests cover:
  - Pydantic schema validation (TradeRecord, BreakRecord, PipelineContext)
  - Data loader normalisation (column aliases, dates, numerics, side)
  - DataIngestionAgent with sample data files
  - StateStore read/write operations
  - GuardrailAgent file checks

Run: pytest tests/unit/test_phase1_ingestion.py -v
===============================================================================
"""

import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from tools.schemas import (
    AuditStatus,
    BreakRecord,
    BreakSeverity,
    BreakType,
    IngestionResult,
    PipelineContext,
    PipelineRun,
    PipelineStatus,
    TradeRecord,
    TradeSide,
    TradeStatus,
)
from tools.data_loader import (
    apply_full_normalisation,
    check_required_columns,
    hash_file,
    load_csv,
    normalise_columns,
    normalise_side,
)

from tools.schemas import PipelineRun

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SAMPLES_DIR  = PROJECT_ROOT / "data" / "samples"


# ── TradeRecord Schema Tests ──────────────────────────────────────────────────

class TestTradeRecord:
    """Validate the canonical trade data contract."""

    def _valid_trade(self, **overrides) -> dict:
        """Return a valid trade dict, with optional field overrides."""
        base = {
            "trade_id":        "TRD20240101001",
            "trade_date":      date(2024, 1, 15),
            "settlement_date": date(2024, 1, 17),
            "ticker":          "AAPL",
            "isin":            "US0378331005",
            "side":            "BUY",
            "quantity":        1000.0,
            "price":           185.50,
            "consideration":   185500.0,
            "counterparty":    "CITI_SECURITIES",
            "broker":          "INSTINET",
            "status":          "CONFIRMED",
        }
        base.update(overrides)
        return base

    def test_valid_trade_creates_successfully(self):
        record = TradeRecord(**self._valid_trade())
        assert record.trade_id == "TRD20240101001"
        assert record.ticker == "AAPL"
        assert record.side == "BUY"

    def test_ticker_normalised_to_uppercase(self):
        record = TradeRecord(**self._valid_trade(ticker="aapl"))
        assert record.ticker == "AAPL"

    def test_isin_normalised_to_uppercase(self):
        record = TradeRecord(**self._valid_trade(isin="us0378331005"))
        assert record.isin == "US0378331005"

    def test_invalid_isin_too_short(self):
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(isin="US123"))

    def test_invalid_isin_no_country_code(self):
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(isin="123456789012"))

    def test_settlement_before_trade_date_rejected(self):
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(
                trade_date=date(2024, 1, 15),
                settlement_date=date(2024, 1, 14),  # Before trade date
            ))

    def test_negative_quantity_rejected(self):
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(quantity=-100.0))

    def test_zero_price_rejected(self):
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(price=0.0))

    def test_invalid_side_rejected(self):
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(side="HOLD"))

    def test_buy_side_accepted(self):
        record = TradeRecord(**self._valid_trade(side="BUY"))
        assert record.side == "BUY"

    def test_sell_side_accepted(self):
        record = TradeRecord(**self._valid_trade(side="SELL"))
        assert record.side == "SELL"

    def test_optional_trader_id_defaults_none(self):
        record = TradeRecord(**self._valid_trade())
        assert record.trader_id is None

    def test_consideration_mismatch_rejected(self):
        """Consideration that is wildly off from qty x price should fail."""
        with pytest.raises(Exception):
            TradeRecord(**self._valid_trade(
                quantity=1000.0,
                price=185.50,
                consideration=99999.0,  # Should be ~185500
            ))


# ── PipelineContext Tests ─────────────────────────────────────────────────────

class TestPipelineContext:
    """Validate the pipeline state contract."""

    def test_context_creates_with_required_fields(self):
        ctx = PipelineContext(
            run_id=str(uuid.uuid4()),
            run_date=date.today(),
        )
        assert ctx.breaks == []
        assert ctx.errors == []
        assert ctx.dry_run is False

    def test_add_error_appends_to_errors(self):
        ctx = PipelineContext(run_id=str(uuid.uuid4()), run_date=date.today())
        ctx.add_error("Something went wrong")
        assert len(ctx.errors) == 1
        assert ctx.has_errors() is True

    def test_has_errors_false_when_empty(self):
        ctx = PipelineContext(run_id=str(uuid.uuid4()), run_date=date.today())
        assert ctx.has_errors() is False

    def test_break_count_by_severity(self):
        ctx = PipelineContext(run_id=str(uuid.uuid4()), run_date=date.today())
        ctx.breaks = [
            BreakRecord(
                break_id=str(uuid.uuid4()), run_id=ctx.run_id,
                break_type="SIDE_BREAK", severity="CRITICAL"
            ),
            BreakRecord(
                break_id=str(uuid.uuid4()), run_id=ctx.run_id,
                break_type="MISSING", severity="HIGH"
            ),
            BreakRecord(
                break_id=str(uuid.uuid4()), run_id=ctx.run_id,
                break_type="PRICE_BREAK", severity="MEDIUM"
            ),
        ]
        assert ctx.break_count_by_severity("CRITICAL") == 1
        assert ctx.break_count_by_severity("HIGH") == 1
        assert ctx.break_count_by_severity("MEDIUM") == 1
        assert ctx.break_count_by_severity("LOW") == 0


# ── Data Loader Tests ─────────────────────────────────────────────────────────

class TestColumnNormalisation:
    """Test column name normalisation logic."""

    def test_lowercase_columns(self):
        df = pd.DataFrame({"TradeID": [1], "TICKER": ["AAPL"]})
        result = normalise_columns(df)
        assert "tradeid" in result.columns or "trade_id" in result.columns

    def test_alias_mapping_applied(self):
        df = pd.DataFrame({"tradeid": ["T001"], "qty": [100], "ticker": ["AAPL"]})
        result = normalise_columns(df)
        assert "trade_id" in result.columns
        assert "quantity" in result.columns

    def test_strips_whitespace_from_column_names(self):
        df = pd.DataFrame({" trade_id ": ["T001"], " ticker ": ["AAPL"]})
        result = normalise_columns(df)
        assert "trade_id" in result.columns
        assert "ticker" in result.columns


class TestSideNormalisation:
    """Test side (BUY/SELL) normalisation."""

    def test_buy_stays_buy(self):
        df = pd.DataFrame({"side": ["BUY", "buy", "Buy"]})
        result = normalise_side(df)
        assert all(result["side"] == "BUY")

    def test_sell_stays_sell(self):
        df = pd.DataFrame({"side": ["SELL", "sell", "Sell"]})
        result = normalise_side(df)
        assert all(result["side"] == "SELL")

    def test_b_maps_to_buy(self):
        df = pd.DataFrame({"side": ["B"]})
        result = normalise_side(df)
        assert result["side"].iloc[0] == "BUY"

    def test_s_maps_to_sell(self):
        df = pd.DataFrame({"side": ["S"]})
        result = normalise_side(df)
        assert result["side"].iloc[0] == "SELL"


class TestRequiredColumns:
    """Test required column presence checks."""

    def test_all_required_columns_present_returns_empty(self):
        df = pd.DataFrame(columns=[
            "trade_id", "trade_date", "settlement_date", "ticker",
            "isin", "side", "quantity", "price", "consideration",
            "counterparty", "broker", "status",
        ])
        missing = check_required_columns(df, "test_source")
        assert missing == []

    def test_missing_columns_returned(self):
        df = pd.DataFrame(columns=["trade_id", "ticker"])
        missing = check_required_columns(df, "test_source")
        assert "trade_date" in missing
        assert "isin" in missing
        assert "side" in missing


class TestFileHashing:
    """Test file integrity hashing."""

    def test_hash_produces_64_char_hex_string(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("trade_id,ticker\nT001,AAPL", encoding="utf-8")
        h = hash_file(f)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_file_same_hash(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("trade_id,ticker\nT001,AAPL", encoding="utf-8")
        assert hash_file(f) == hash_file(f)

    def test_different_content_different_hash(self, tmp_path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("content_a", encoding="utf-8")
        f2.write_text("content_b", encoding="utf-8")
        assert hash_file(f1) != hash_file(f2)


# ── CSV Loader Tests ──────────────────────────────────────────────────────────

class TestCSVLoader:
    """Test CSV loading with sample data files."""

    def test_load_source_a_csv(self):
        path = SAMPLES_DIR / "source_a_trades.csv"
        if not path.exists():
            pytest.skip("Run generate_sample_data.py first")

        df = load_csv(path, "source_a")
        assert len(df) > 0
        assert "trade_id" in df.columns
        assert "ticker" in df.columns
        assert "side" in df.columns
        assert "quantity" in df.columns

    def test_load_source_b_csv(self):
        path = SAMPLES_DIR / "source_b_trades.csv"
        if not path.exists():
            pytest.skip("Run generate_sample_data.py first")

        df = load_csv(path, "source_b")
        assert len(df) > 0

    def test_side_column_normalised_to_buy_sell(self):
        path = SAMPLES_DIR / "source_a_trades.csv"
        if not path.exists():
            pytest.skip("Run generate_sample_data.py first")

        df = load_csv(path, "source_a")
        assert set(df["side"].unique()).issubset({"BUY", "SELL"})

    def test_source_label_added(self):
        path = SAMPLES_DIR / "source_a_trades.csv"
        if not path.exists():
            pytest.skip("Run generate_sample_data.py first")

        df = load_csv(path, "source_a")
        assert "source" in df.columns
        assert all(df["source"] == "source_a")

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_csv(tmp_path / "nonexistent.csv", "test")

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("", encoding="utf-8")
        with pytest.raises(Exception):
            load_csv(f, "test")


# ── StateStore Tests ──────────────────────────────────────────────────────────

class TestStateStore:
    """Test SQLite state persistence."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create a temporary database for testing."""
        db_path = tmp_path / "test.db"
        schema = (PROJECT_ROOT / "db" / "schema.sql").read_text(encoding="utf-8")
        conn = sqlite3.connect(db_path)
        conn.executescript(schema)
        conn.commit()
        conn.close()

        from tools.state_store import StateStore
        return StateStore(db_path)

    def test_create_and_retrieve_run(self, store):
        run = PipelineRun(
            run_id=str(uuid.uuid4()),
            run_date=date.today(),
            started_at=datetime.now(timezone.utc),
            status="RUNNING",
        )
        store.create_run(run)
        retrieved = store.get_run(run.run_id)
        assert retrieved is not None
        assert retrieved["run_id"] == run.run_id

    def test_complete_run_updates_status(self, store):
        run_id = str(uuid.uuid4())
        run = PipelineRun(
            run_id=run_id,
            run_date=date.today(),
            started_at=datetime.now(timezone.utc),
            status="RUNNING",
        )
        store.create_run(run)
        store.complete_run(run_id, "COMPLETED", total_breaks=5)
        retrieved = store.get_run(run_id)
        assert retrieved["status"] == "COMPLETED"
        assert retrieved["total_breaks"] == 5

    def test_write_and_retrieve_audit_entry(self, store):
        run_id = str(uuid.uuid4())
            # Must create the pipeline run first — FK constraint requires it
 
        run = PipelineRun(
            run_id=run_id,
            run_date=date.today(),
            started_at=datetime.now(timezone.utc),
        )
        store.create_run(run)
        
        store.quick_audit(
            agent_name="TestAgent",
            action="test_action",
            status=AuditStatus.SUCCESS,
            run_id=run_id,
            detail={"key": "value"},
            duration_ms=42,
        )
        entries = store.get_audit_for_run(run_id)
        assert len(entries) == 1
        assert entries[0]["agent_name"] == "TestAgent"
        assert entries[0]["action"] == "test_action"
        assert entries[0]["duration_ms"] == 42

    def test_write_breaks_bulk(self, store):
        run_id = str(uuid.uuid4())
        run = PipelineRun(
            run_id=run_id,
            run_date=date.today(),
            started_at=datetime.now(timezone.utc),
        )
        store.create_run(run)

        breaks = [
            BreakRecord(
                break_id=str(uuid.uuid4()),
                run_id=run_id,
                trade_id=f"TRD{i:04d}",
                break_type="MISSING",
                severity="HIGH",
            )
            for i in range(3)
        ]
        store.write_breaks(breaks)
        retrieved = store.get_breaks_for_run(run_id)
        assert len(retrieved) == 3

    def test_database_not_found_raises(self, tmp_path):
        from tools.state_store import StateStore
        with pytest.raises(FileNotFoundError):
            StateStore(tmp_path / "nonexistent.db")
