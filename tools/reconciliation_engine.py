"""
tools/reconciliation_engine.py
===============================================================================
Core reconciliation matching logic -- pure functions, no agent dependencies.

This is the matching engine that compares two sets of trade records and
identifies all 6 break types.

CICS equivalent: The core comparison program that reads matched/unmatched
records from a VSAM KSDS and writes exception records for each discrepancy.

Design principle: Pure functions only in this module.
  - No side effects
  - No database calls
  - No agent state
  - Takes DataFrames in, returns BreakRecord lists out
  - Fully testable in isolation

The ReconciliationAgent (agents/reconciliation.py) wraps these functions
and handles the pipeline orchestration around them.
===============================================================================
"""

import uuid
from datetime import date
from typing import Optional

import pandas as pd

from tools.schemas import BreakRecord, BreakSeverity, BreakType


# ── Break factory helper ──────────────────────────────────────────────────────

def make_break(
    run_id: str,
    break_type: BreakType | str,
    severity: BreakSeverity | str,
    trade_id: Optional[str] = None,
    source_a_value: Optional[str] = None,
    source_b_value: Optional[str] = None,
    difference: Optional[str] = None,
) -> BreakRecord:
    """Create a BreakRecord with a fresh UUID."""
    bt = break_type.value if hasattr(break_type, "value") else str(break_type)
    sv = severity.value if hasattr(severity, "value") else str(severity)
    
    return BreakRecord(
        break_id=str(uuid.uuid4()),
        run_id=run_id,
        trade_id=trade_id,
        break_type=bt,
        severity=sv,
        source_a_value=source_a_value,
        source_b_value=source_b_value,
        difference=difference,
    )


# ── Step 1: Duplicate Detection ───────────────────────────────────────────────

def detect_duplicates(
    df: pd.DataFrame,
    source_label: str,
    run_id: str,
) -> tuple[pd.DataFrame, list[BreakRecord]]:
    """
    Find duplicate trade_ids within a single source.

    A duplicate means the same Trade ID appears more than once in one source.
    This is always a HIGH severity break -- it indicates a data quality problem
    in the source system before reconciliation even begins.

    Returns:
        - DataFrame with duplicates removed (keeping first occurrence)
        - List of DUPLICATE BreakRecords
    """
    breaks: list[BreakRecord] = []

    duplicate_mask = df.duplicated(subset=["trade_id"], keep=False)
    duplicates = df[duplicate_mask]

    if duplicates.empty:
        return df, breaks

    # Group by trade_id to report each duplicate set once
    for trade_id, group in duplicates.groupby("trade_id"):
        breaks.append(make_break(
            run_id=run_id,
            break_type=BreakType.DUPLICATE,
            severity=BreakSeverity.HIGH,
            trade_id=str(trade_id),
            source_a_value=f"{source_label}: {len(group)} occurrences",
            source_b_value=None,
            difference=f"Expected 1, found {len(group)}",
        ))

    # Keep only the first occurrence of each trade_id for downstream matching
    df_deduped = df.drop_duplicates(subset=["trade_id"], keep="first")
    return df_deduped, breaks


# ── Step 2: Missing Trade Detection ──────────────────────────────────────────

def detect_missing_trades(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    run_id: str,
) -> list[BreakRecord]:
    """
    Find trades that exist in one source but not the other.

    MISSING breaks are HIGH severity -- a trade that one party recorded
    and the other didn't is a serious reconciliation failure.

    Uses set difference on trade_id as the primary key.
    """
    breaks: list[BreakRecord] = []

    ids_a = set(df_a["trade_id"].astype(str)) if "trade_id" in df_a.columns else set()
    ids_b = set(df_b["trade_id"].astype(str)) if "trade_id" in df_b.columns else set()

    # In A but not in B
    for trade_id in sorted(ids_a - ids_b):
        row = df_a[df_a["trade_id"].astype(str) == trade_id].iloc[0]
        breaks.append(make_break(
            run_id=run_id,
            break_type=BreakType.MISSING,
            severity=BreakSeverity.HIGH,
            trade_id=trade_id,
            source_a_value=f"{row.get('ticker','?')} {row.get('side','?')} {row.get('quantity','?')}@{row.get('price','?')}",
            source_b_value="NOT FOUND",
            difference="Trade present in Source A, absent in Source B",
        ))

    # In B but not in A
    for trade_id in sorted(ids_b - ids_a):
        row = df_b[df_b["trade_id"].astype(str) == trade_id].iloc[0]
        breaks.append(make_break(
            run_id=run_id,
            break_type=BreakType.MISSING,
            severity=BreakSeverity.HIGH,
            trade_id=trade_id,
            source_a_value="NOT FOUND",
            source_b_value=f"{row.get('ticker','?')} {row.get('side','?')} {row.get('quantity','?')}@{row.get('price','?')}",
            difference="Trade present in Source B, absent in Source A",
        ))

    return breaks


# ── Step 3: Matched Trade Comparison ─────────────────────────────────────────

def compare_matched_trades(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    run_id: str,
    price_tolerance: float = 0.01,
    consideration_tolerance: float = 1.00,
    qty_tolerance: float = 0,
    settlement_tolerance_days: int = 0,
) -> tuple[int, list[BreakRecord]]:
    """
    Compare trades that exist in both sources and detect field-level breaks.

    Merges on trade_id and compares each matched pair for:
      - SIDE_BREAK    (Critical)
      - QTY_BREAK     (High)
      - PRICE_BREAK   (Medium)
      - SETTLE_BREAK  (Medium)

    Returns:
        - Count of clean matched trades (no breaks)
        - List of BreakRecords for all field-level breaks found
    """
    breaks: list[BreakRecord] = []

    # Find trade_ids present in both sources
    ids_a = set(df_a["trade_id"].astype(str)) if "trade_id" in df_a.columns else set()
    ids_b = set(df_b["trade_id"].astype(str)) if "trade_id" in df_b.columns else set()
    
    common_ids = ids_a & ids_b

    if not common_ids:
        return 0, breaks

    # Filter to matched trades only
    matched_a = df_a[df_a["trade_id"].astype(str).isin(common_ids)].copy()
    matched_b = df_b[df_b["trade_id"].astype(str).isin(common_ids)].copy()

    # Merge on trade_id with suffixes to distinguish source columns
    merged = matched_a.merge(
        matched_b,
        on="trade_id",
        suffixes=("_a", "_b"),
        how="inner",
    )

    clean_count = 0

    for _, row in merged.iterrows():
        trade_id = str(row["trade_id"])
        row_breaks: list[BreakRecord] = []

        # ── SIDE_BREAK (Critical) ─────────────────────────────────────────
        side_a = str(row.get("side_a", "")).upper().strip()
        side_b = str(row.get("side_b", "")).upper().strip()
        if side_a and side_b and side_a != side_b:
            row_breaks.append(make_break(
                run_id=run_id,
                break_type=BreakType.SIDE_BREAK,
                severity=BreakSeverity.CRITICAL,
                trade_id=trade_id,
                source_a_value=side_a,
                source_b_value=side_b,
                difference=f"Source A={side_a}, Source B={side_b}",
            ))

        # ── QTY_BREAK (High) ──────────────────────────────────────────────
        try:
            qty_a = float(row.get("quantity_a", 0))
            qty_b = float(row.get("quantity_b", 0))
            if abs(qty_a - qty_b) > qty_tolerance:
                row_breaks.append(make_break(
                    run_id=run_id,
                    break_type=BreakType.QTY_BREAK,
                    severity=BreakSeverity.HIGH,
                    trade_id=trade_id,
                    source_a_value=str(qty_a),
                    source_b_value=str(qty_b),
                    difference=str(qty_a - qty_b),
                ))
        except (TypeError, ValueError):
            pass

        # ── PRICE_BREAK (Medium) ──────────────────────────────────────────
        try:
            price_a = float(row.get("price_a", 0))
            price_b = float(row.get("price_b", 0))
            if abs(price_a - price_b) > price_tolerance:
                row_breaks.append(make_break(
                    run_id=run_id,
                    break_type=BreakType.PRICE_BREAK,
                    severity=BreakSeverity.MEDIUM,
                    trade_id=trade_id,
                    source_a_value=str(price_a),
                    source_b_value=str(price_b),
                    difference=f"{price_a - price_b:+.4f}",
                ))
        except (TypeError, ValueError):
            pass

        # ── SETTLE_BREAK (Medium) ─────────────────────────────────────────
        try:
            settle_a = row.get("settlement_date_a")
            settle_b = row.get("settlement_date_b")
            if settle_a is not None and settle_b is not None:
                # Convert to date objects if they're strings
                if isinstance(settle_a, str):
                    settle_a = date.fromisoformat(settle_a)
                if isinstance(settle_b, str):
                    settle_b = date.fromisoformat(settle_b)
                delta = abs((settle_a - settle_b).days)
                if delta > settlement_tolerance_days:
                    row_breaks.append(make_break(
                        run_id=run_id,
                        break_type=BreakType.SETTLE_BREAK,
                        severity=BreakSeverity.MEDIUM,
                        trade_id=trade_id,
                        source_a_value=str(settle_a),
                        source_b_value=str(settle_b),
                        difference=f"{delta} day(s)",
                    ))
        except (TypeError, ValueError, AttributeError):
            pass

        if row_breaks:
            breaks.extend(row_breaks)
        else:
            clean_count += 1

    return clean_count, breaks


# ── Main Reconciliation Entry Point ──────────────────────────────────────────

def reconcile(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    run_id: str,
    tolerances: dict,
) -> tuple[int, list[BreakRecord]]:
    """
    Full reconciliation pipeline -- runs all detection steps in order.

    Steps:
      1. Detect duplicates in each source
      2. Detect missing trades
      3. Compare matched trades for field-level breaks

    Args:
        df_a:       Normalised DataFrame from source A
        df_b:       Normalised DataFrame from source B
        run_id:     Pipeline run ID for break records
        tolerances: Dict of tolerance values from config/tolerance.yaml

    Returns:
        - Count of cleanly matched trades
        - Complete list of all BreakRecords found
    """
    all_breaks: list[BreakRecord] = []

    defaults = tolerances.get("defaults", {})
    price_tol       = float(defaults.get("price_tolerance", 0.01))
    consideration_tol = float(defaults.get("consideration_tolerance", 1.00))
    qty_tol         = float(defaults.get("quantity_tolerance", 0))
    settle_tol      = int(defaults.get("settlement_date_tolerance_days", 0))

    # Step 1: Duplicate detection
    df_a_clean, dup_breaks_a = detect_duplicates(df_a.copy(), "source_a", run_id)
    df_b_clean, dup_breaks_b = detect_duplicates(df_b.copy(), "source_b", run_id)
    all_breaks.extend(dup_breaks_a)
    all_breaks.extend(dup_breaks_b)

    # Step 2: Missing trade detection (on deduplicated data)
    missing_breaks = detect_missing_trades(df_a_clean, df_b_clean, run_id)
    all_breaks.extend(missing_breaks)

    # Step 3: Field-level comparison of matched trades
    clean_count, field_breaks = compare_matched_trades(
        df_a_clean, df_b_clean, run_id,
        price_tolerance=price_tol,
        consideration_tolerance=consideration_tol,
        qty_tolerance=qty_tol,
        settlement_tolerance_days=settle_tol,
    )
    all_breaks.extend(field_breaks)

    return clean_count, all_breaks
