"""
tests/unit/test_phase2_reconciliation.py
===============================================================================
Phase 2 unit tests -- reconciliation engine and all 6 break types.

Tests cover every break type in isolation, tolerance logic, edge cases,
and the full reconciliation pipeline end-to-end with sample data.

Run: pytest tests/unit/test_phase2_reconciliation.py -v
===============================================================================
"""

import uuid
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from tools.reconciliation_engine import (
    compare_matched_trades,
    detect_duplicates,
    detect_missing_trades,
    reconcile,
)
from tools.schemas import BreakSeverity, BreakType


# ── Test Data Factories ───────────────────────────────────────────────────────

def make_trade_df(trades: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from a list of trade dicts."""
    return pd.DataFrame(trades)


def trade(
    trade_id: str = "T001",
    ticker: str = "AAPL",
    side: str = "BUY",
    quantity: float = 1000.0,
    price: float = 185.50,
    settlement_date = date(2024, 1, 17),
    trade_date = date(2024, 1, 15),
    isin: str = "US0378331005",
    counterparty: str = "CITI",
    broker: str = "INSTINET",
    status: str = "CONFIRMED",
    source: str = "source_a",
    **kwargs,
) -> dict:
    """Build a single trade dict with sensible defaults."""
    return {
        "trade_id":        trade_id,
        "ticker":          ticker,
        "side":            side,
        "quantity":        quantity,
        "price":           price,
        "consideration":   quantity * price,
        "settlement_date": settlement_date,
        "trade_date":      trade_date,
        "isin":            isin,
        "counterparty":    counterparty,
        "broker":          broker,
        "status":          status,
        "source":          source,
        **kwargs,
    }


def run_id() -> str:
    return str(uuid.uuid4())


DEFAULT_TOLERANCES = {
    "defaults": {
        "price_tolerance":                0.01,
        "consideration_tolerance":        1.00,
        "quantity_tolerance":             0,
        "settlement_date_tolerance_days": 0,
    }
}


# ── Duplicate Detection Tests ─────────────────────────────────────────────────

class TestDuplicateDetection:

    def test_no_duplicates_returns_empty_breaks(self):
        df = make_trade_df([trade("T001"), trade("T002")])
        deduped, breaks = detect_duplicates(df, "source_a", run_id())
        assert breaks == []
        assert len(deduped) == 2

    def test_duplicate_trade_id_detected(self):
        df = make_trade_df([trade("T001"), trade("T001"), trade("T002")])
        deduped, breaks = detect_duplicates(df, "source_a", run_id())
        assert len(breaks) == 1
        assert breaks[0].break_type == "DUPLICATE"

    def test_duplicate_severity_is_high(self):
        df = make_trade_df([trade("T001"), trade("T001")])
        _, breaks = detect_duplicates(df, "source_a", run_id())
        assert breaks[0].severity == "HIGH"

    def test_duplicate_removed_from_output_df(self):
        df = make_trade_df([trade("T001"), trade("T001"), trade("T002")])
        deduped, _ = detect_duplicates(df, "source_a", run_id())
        assert len(deduped) == 2
        assert list(deduped["trade_id"]) == ["T001", "T002"]

    def test_three_duplicates_reported_as_one_break(self):
        """Three rows with same ID = 1 break (one per unique duplicate ID)."""
        df = make_trade_df([trade("T001"), trade("T001"), trade("T001")])
        _, breaks = detect_duplicates(df, "source_a", run_id())
        assert len(breaks) == 1

    def test_duplicate_trade_id_in_break_record(self):
        df = make_trade_df([trade("T001"), trade("T001")])
        _, breaks = detect_duplicates(df, "source_a", run_id())
        assert breaks[0].trade_id == "T001"


# ── Missing Trade Detection Tests ─────────────────────────────────────────────

class TestMissingTradeDetection:

    def test_no_missing_trades_returns_empty(self):
        df_a = make_trade_df([trade("T001"), trade("T002")])
        df_b = make_trade_df([trade("T001", source="source_b"), trade("T002", source="source_b")])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert breaks == []

    def test_trade_in_a_not_in_b_detected(self):
        df_a = make_trade_df([trade("T001"), trade("T002")])
        df_b = make_trade_df([trade("T001", source="source_b")])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert len(breaks) == 1
        assert breaks[0].break_type == "MISSING"
        assert breaks[0].trade_id == "T002"

    def test_trade_in_b_not_in_a_detected(self):
        df_a = make_trade_df([trade("T001")])
        df_b = make_trade_df([trade("T001", source="source_b"), trade("T999", source="source_b")])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert len(breaks) == 1
        assert breaks[0].trade_id == "T999"

    def test_missing_severity_is_high(self):
        df_a = make_trade_df([trade("T001")])
        df_b = make_trade_df([])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert all(b.severity == "HIGH" for b in breaks)

    def test_multiple_missing_trades_all_detected(self):
        df_a = make_trade_df([trade("T001"), trade("T002"), trade("T003")])
        df_b = make_trade_df([trade("T001", source="source_b")])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert len(breaks) == 2
        missing_ids = {b.trade_id for b in breaks}
        assert missing_ids == {"T002", "T003"}

    def test_source_b_value_is_not_found_for_a_only_trade(self):
        df_a = make_trade_df([trade("T001")])
        df_b = make_trade_df([])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert breaks[0].source_b_value == "NOT FOUND"

    def test_source_a_value_is_not_found_for_b_only_trade(self):
        df_a = make_trade_df([])
        df_b = make_trade_df([trade("T001", source="source_b")])
        breaks = detect_missing_trades(df_a, df_b, run_id())
        assert breaks[0].source_a_value == "NOT FOUND"


# ── Side Break Tests ──────────────────────────────────────────────────────────

class TestSideBreak:

    def test_matching_sides_no_break(self):
        df_a = make_trade_df([trade("T001", side="BUY")])
        df_b = make_trade_df([trade("T001", side="BUY", source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        side_breaks = [b for b in breaks if b.break_type == "SIDE_BREAK"]
        assert side_breaks == []

    def test_buy_vs_sell_is_side_break(self):
        df_a = make_trade_df([trade("T001", side="BUY")])
        df_b = make_trade_df([trade("T001", side="SELL", source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        side_breaks = [b for b in breaks if b.break_type == "SIDE_BREAK"]
        assert len(side_breaks) == 1

    def test_side_break_severity_is_critical(self):
        df_a = make_trade_df([trade("T001", side="BUY")])
        df_b = make_trade_df([trade("T001", side="SELL", source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        side_breaks = [b for b in breaks if b.break_type == "SIDE_BREAK"]
        assert side_breaks[0].severity == "CRITICAL"

    def test_side_break_records_both_values(self):
        df_a = make_trade_df([trade("T001", side="BUY")])
        df_b = make_trade_df([trade("T001", side="SELL", source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        sb = [b for b in breaks if b.break_type == "SIDE_BREAK"][0]
        assert sb.source_a_value == "BUY"
        assert sb.source_b_value == "SELL"


# ── Quantity Break Tests ──────────────────────────────────────────────────────

class TestQuantityBreak:

    def test_matching_quantities_no_break(self):
        df_a = make_trade_df([trade("T001", quantity=1000)])
        df_b = make_trade_df([trade("T001", quantity=1000, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        qty_breaks = [b for b in breaks if b.break_type == "QTY_BREAK"]
        assert qty_breaks == []

    def test_different_quantities_is_qty_break(self):
        df_a = make_trade_df([trade("T001", quantity=1000)])
        df_b = make_trade_df([trade("T001", quantity=1100, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        qty_breaks = [b for b in breaks if b.break_type == "QTY_BREAK"]
        assert len(qty_breaks) == 1

    def test_qty_break_severity_is_high(self):
        df_a = make_trade_df([trade("T001", quantity=1000)])
        df_b = make_trade_df([trade("T001", quantity=1100, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        qty_breaks = [b for b in breaks if b.break_type == "QTY_BREAK"]
        assert qty_breaks[0].severity == "HIGH"

    def test_qty_within_zero_tolerance_is_break(self):
        """Default tolerance is 0 -- any difference is a break."""
        df_a = make_trade_df([trade("T001", quantity=1000)])
        df_b = make_trade_df([trade("T001", quantity=1001, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id(), qty_tolerance=0)
        qty_breaks = [b for b in breaks if b.break_type == "QTY_BREAK"]
        assert len(qty_breaks) == 1

    def test_qty_difference_recorded(self):
        df_a = make_trade_df([trade("T001", quantity=1000)])
        df_b = make_trade_df([trade("T001", quantity=1100, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        qb = [b for b in breaks if b.break_type == "QTY_BREAK"][0]
        assert qb.source_a_value == "1000.0"
        assert qb.source_b_value == "1100.0"


# ── Price Break Tests ─────────────────────────────────────────────────────────

class TestPriceBreak:

    def test_matching_prices_no_break(self):
        df_a = make_trade_df([trade("T001", price=185.50)])
        df_b = make_trade_df([trade("T001", price=185.50, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        price_breaks = [b for b in breaks if b.break_type == "PRICE_BREAK"]
        assert price_breaks == []

    def test_price_within_tolerance_no_break(self):
        df_a = make_trade_df([trade("T001", price=185.50)])
        df_b = make_trade_df([trade("T001", price=185.505, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id(), price_tolerance=0.01)
        price_breaks = [b for b in breaks if b.break_type == "PRICE_BREAK"]
        assert price_breaks == []

    def test_price_outside_tolerance_is_break(self):
        df_a = make_trade_df([trade("T001", price=185.50)])
        df_b = make_trade_df([trade("T001", price=185.60, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id(), price_tolerance=0.01)
        price_breaks = [b for b in breaks if b.break_type == "PRICE_BREAK"]
        assert len(price_breaks) == 1

    def test_price_break_severity_is_medium(self):
        df_a = make_trade_df([trade("T001", price=185.50)])
        df_b = make_trade_df([trade("T001", price=186.00, source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id(), price_tolerance=0.01)
        price_breaks = [b for b in breaks if b.break_type == "PRICE_BREAK"]
        assert price_breaks[0].severity == "MEDIUM"


# ── Settlement Date Break Tests ───────────────────────────────────────────────

class TestSettlementBreak:

    def test_matching_settlement_dates_no_break(self):
        df_a = make_trade_df([trade("T001", settlement_date=date(2024, 1, 17))])
        df_b = make_trade_df([trade("T001", settlement_date=date(2024, 1, 17), source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        settle_breaks = [b for b in breaks if b.break_type == "SETTLE_BREAK"]
        assert settle_breaks == []

    def test_different_settlement_dates_is_break(self):
        df_a = make_trade_df([trade("T001", settlement_date=date(2024, 1, 17))])
        df_b = make_trade_df([trade("T001", settlement_date=date(2024, 1, 18), source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id(), settlement_tolerance_days=0)
        settle_breaks = [b for b in breaks if b.break_type == "SETTLE_BREAK"]
        assert len(settle_breaks) == 1

    def test_settlement_break_severity_is_medium(self):
        df_a = make_trade_df([trade("T001", settlement_date=date(2024, 1, 17))])
        df_b = make_trade_df([trade("T001", settlement_date=date(2024, 1, 18), source="source_b")])
        _, breaks = compare_matched_trades(df_a, df_b, run_id())
        settle_breaks = [b for b in breaks if b.break_type == "SETTLE_BREAK"]
        assert settle_breaks[0].severity == "MEDIUM"

    def test_settlement_within_tolerance_no_break(self):
        df_a = make_trade_df([trade("T001", settlement_date=date(2024, 1, 17))])
        df_b = make_trade_df([trade("T001", settlement_date=date(2024, 1, 18), source="source_b")])
        _, breaks = compare_matched_trades(
            df_a, df_b, run_id(), settlement_tolerance_days=1
        )
        settle_breaks = [b for b in breaks if b.break_type == "SETTLE_BREAK"]
        assert settle_breaks == []


# ── Clean Match Tests ─────────────────────────────────────────────────────────

class TestCleanMatches:

    def test_identical_trades_count_as_clean(self):
        df_a = make_trade_df([trade("T001"), trade("T002")])
        df_b = make_trade_df([
            trade("T001", source="source_b"),
            trade("T002", source="source_b"),
        ])
        clean_count, breaks = compare_matched_trades(df_a, df_b, run_id())
        assert clean_count == 2
        assert breaks == []

    def test_mixed_clean_and_breaks(self):
        df_a = make_trade_df([trade("T001"), trade("T002", side="BUY")])
        df_b = make_trade_df([
            trade("T001", source="source_b"),
            trade("T002", side="SELL", source="source_b"),
        ])
        clean_count, breaks = compare_matched_trades(df_a, df_b, run_id())
        assert clean_count == 1
        assert len(breaks) == 1


# ── Full Reconciliation Pipeline Tests ───────────────────────────────────────

class TestFullReconciliation:

    def test_clean_data_no_breaks(self):
        df_a = make_trade_df([trade("T001"), trade("T002"), trade("T003")])
        df_b = make_trade_df([
            trade("T001", source="source_b"),
            trade("T002", source="source_b"),
            trade("T003", source="source_b"),
        ])
        clean, breaks = reconcile(df_a, df_b, run_id(), DEFAULT_TOLERANCES)
        assert clean == 3
        assert breaks == []

    def test_all_six_break_types_detected(self):
        """Build a dataset that triggers all 6 break types."""
        df_a = make_trade_df([
            trade("T001"),                                          # Clean
            trade("T002"),                                          # Missing in B
            trade("T003", side="BUY"),                             # Side break
            trade("T004", quantity=1000),                          # Qty break
            trade("T005", price=185.50),                           # Price break
            trade("T006", settlement_date=date(2024, 1, 17)),      # Settle break
            trade("T007"), trade("T007"),                          # Duplicate in A
        ])
        df_b = make_trade_df([
            trade("T001", source="source_b"),                      # Clean match
            # T002 missing
            trade("T003", side="SELL", source="source_b"),         # Side break
            trade("T004", quantity=1100, source="source_b"),       # Qty break
            trade("T005", price=186.00, source="source_b"),        # Price break
            trade("T006", settlement_date=date(2024, 1, 18), source="source_b"),  # Settle break
            trade("T008", source="source_b"), trade("T008", source="source_b"),   # Duplicate in B
        ])

        clean, breaks = reconcile(df_a, df_b, run_id(), DEFAULT_TOLERANCES)

        break_types = {b.break_type for b in breaks}
        assert "DUPLICATE"    in break_types
        assert "MISSING"      in break_types
        assert "SIDE_BREAK"   in break_types
        assert "QTY_BREAK"    in break_types
        assert "PRICE_BREAK"  in break_types
        assert "SETTLE_BREAK" in break_types

    def test_reconcile_with_sample_data_files(self):
        """Integration test using the actual sample CSV files."""
        from pathlib import Path
        from tools.data_loader import load_csv

        samples = Path("data/samples")
        if not (samples / "source_a_trades.csv").exists():
            pytest.skip("Run generate_sample_data.py first")

        df_a = load_csv(samples / "source_a_trades.csv", "source_a")
        df_b = load_csv(samples / "source_b_trades.csv", "source_b")

        clean, breaks = reconcile(df_a, df_b, run_id(), DEFAULT_TOLERANCES)

        # Sample data is generated with intentional breaks -- there should be some
        assert isinstance(clean, int)
        assert isinstance(breaks, list)
        assert clean >= 0

        # Verify all breaks have required fields
        for b in breaks:
            assert b.break_id is not None
            assert b.break_type in [
                "DUPLICATE", "MISSING", "SIDE_BREAK",
                "QTY_BREAK", "PRICE_BREAK", "SETTLE_BREAK"
            ]
            assert b.severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    def test_empty_source_b_all_missing(self):
        df_a = make_trade_df([trade("T001"), trade("T002")])
        df_b = make_trade_df([])
        clean, breaks = reconcile(df_a, df_b, run_id(), DEFAULT_TOLERANCES)
        assert clean == 0
        missing = [b for b in breaks if b.break_type == "MISSING"]
        assert len(missing) == 2

    def test_break_records_have_run_id(self):
        rid = run_id()
        df_a = make_trade_df([trade("T001", side="BUY")])
        df_b = make_trade_df([trade("T001", side="SELL", source="source_b")])
        _, breaks = reconcile(df_a, df_b, rid, DEFAULT_TOLERANCES)
        for b in breaks:
            assert b.run_id == rid

    def test_all_breaks_have_unique_ids(self):
        df_a = make_trade_df([trade("T001"), trade("T002"), trade("T003")])
        df_b = make_trade_df([trade("T001", source="source_b")])
        _, breaks = reconcile(df_a, df_b, run_id(), DEFAULT_TOLERANCES)
        ids = [b.break_id for b in breaks]
        assert len(ids) == len(set(ids))  # All unique
