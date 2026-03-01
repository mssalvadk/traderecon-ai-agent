"""
tools/data_loader.py
===============================================================================
Multi-source data loader -- reads CSV, Excel, flat files and SQLite sources
and normalises them into a standard pandas DataFrame ready for validation.

This is the CICS File Control equivalent -- EXEC CICS READ abstracted into
a clean Python interface. The caller doesn't care whether data came from
a CSV or an Excel file -- they get the same normalised shape either way.

Key design decisions:
  - All file reads specify encoding="utf-8" explicitly (Windows safety)
  - Column name normalisation strips whitespace and lowercases everything
  - Date columns are parsed to datetime.date objects
  - Numeric columns are coerced to float with error handling
  - Every loader returns a DataFrame with the canonical column set
===============================================================================
"""

import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


# ── Canonical column mapping ──────────────────────────────────────────────────
# Maps common source column name variations to our canonical names.
# Source systems rarely use the exact same column names we do.

COLUMN_ALIASES: dict[str, str] = {
    # Trade ID variations
    "tradeid":         "trade_id",
    "trade_ref":       "trade_id",
    "reference":       "trade_id",
    "ref":             "trade_id",
    "id":              "trade_id",

    # Date variations
    "tradedate":       "trade_date",
    "trade date":      "trade_date",
    "execution_date":  "trade_date",
    "exec_date":       "trade_date",

    # Settlement date variations
    "settlementdate":  "settlement_date",
    "settle_date":     "settlement_date",
    "settledate":      "settlement_date",
    "value_date":      "settlement_date",

    # Side variations
    "direction":       "side",
    "buysell":         "side",
    "buy_sell":        "side",
    "action":          "side",

    # Quantity variations
    "qty":             "quantity",
    "shares":          "quantity",
    "units":           "quantity",
    "nominal":         "quantity",
    "face_value":      "quantity",

    # Price variations
    "exec_price":      "price",
    "execution_price": "price",
    "unit_price":      "price",
    "clean_price":     "price",

    # Consideration variations
    "gross_amount":    "consideration",
    "net_amount":      "consideration",
    "amount":          "consideration",
    "value":           "consideration",
    "notional":        "consideration",

    # Counterparty variations
    "cpty":            "counterparty",
    "counter_party":   "counterparty",
    "cp":              "counterparty",

    # Broker variations
    "executing_broker":"broker",
    "exec_broker":     "broker",
    "agent":           "broker",
}

# Canonical columns that must be present after normalisation
REQUIRED_COLUMNS = [
    "trade_id", "trade_date", "settlement_date", "ticker",
    "isin", "side", "quantity", "price", "consideration",
    "counterparty", "broker", "status",
]

# Date columns to parse
DATE_COLUMNS = ["trade_date", "settlement_date"]

# Numeric columns to coerce
NUMERIC_COLUMNS = ["quantity", "price", "consideration"]


# ── File Hashing ──────────────────────────────────────────────────────────────

def hash_file(file_path: str | Path) -> str:
    """
    Compute SHA256 hash of a file for integrity verification.

    Written to audit log so we can prove exactly which file was processed.
    Equivalent to a CICS file integrity check before processing begins.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# ── Column Normalisation ──────────────────────────────────────────────────────

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise column names to canonical form.

    Strips whitespace, lowercases, and applies alias mapping so
    source systems with different column names all produce the same output.
    """
    # Strip whitespace and lowercase all column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Apply alias mapping
    df = df.rename(columns=COLUMN_ALIASES)

    return df


def normalise_side(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise side column to BUY/SELL uppercase."""
    if "side" in df.columns:
        df["side"] = (
            df["side"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"B": "BUY", "S": "SELL", "1": "BUY", "-1": "SELL"})
        )
    return df


def normalise_dates(df: pd.DataFrame, date_format: Optional[str] = None) -> pd.DataFrame:
    """Parse date columns to datetime.date objects."""
    for col in DATE_COLUMNS:
        if col in df.columns:
            try:
                if date_format:
                    df[col] = pd.to_datetime(df[col], format=date_format).dt.date
                else:
                    #df[col] = pd.to_datetime(df[col], infer_datetime_format=True).dt.date
                    df[col] = pd.to_datetime(df[col]).dt.date
            except Exception as e:
                raise ValueError(f"Failed to parse date column '{col}': {e}") from e
    return df


def normalise_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce numeric columns to float, replacing unparseable values with NaN."""
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def apply_full_normalisation(
    df: pd.DataFrame,
    source_name: str,
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Apply the complete normalisation pipeline to a raw DataFrame.

    Steps:
      1. Normalise column names
      2. Normalise side values
      3. Parse dates
      4. Coerce numerics
      5. Add source label
      6. Strip string whitespace
      7. Drop fully empty rows
    """
    df = normalise_columns(df)
    df = normalise_side(df)
    df = normalise_dates(df, date_format)
    df = normalise_numerics(df)

    # Add source label for traceability
    df["source"] = source_name

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include=["object", "str"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # Drop completely empty rows
    df = df.dropna(how="all")

    return df


def check_required_columns(df: pd.DataFrame, source_name: str) -> list[str]:
    """
    Check that all required columns are present after normalisation.
    Returns list of missing column names (empty list = all present).
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return missing


# ── Source-Specific Loaders ───────────────────────────────────────────────────

def load_csv(
    file_path: str | Path,
    source_name: str,
    encoding: str = "utf-8",
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load a CSV file and normalise to canonical schema.

    Args:
        file_path:   Path to the CSV file
        source_name: Label for this source (e.g. "source_a")
        encoding:    File encoding (always specify -- never rely on system default)
        date_format: Optional explicit date format string

    Returns:
        Normalised DataFrame with canonical column names
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path, encoding=encoding, dtype=str)  # Read as str first, coerce later

    if df.empty:
        raise ValueError(f"CSV file is empty: {path}")

    df = apply_full_normalisation(df, source_name, date_format)

    missing = check_required_columns(df, source_name)
    if missing:
        raise ValueError(
            f"Source '{source_name}' missing required columns after normalisation: {missing}"
        )

    return df


def load_excel(
    file_path: str | Path,
    source_name: str,
    sheet_name: str | int = 0,
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load an Excel file (.xlsx or .xls) and normalise to canonical schema.

    Args:
        file_path:   Path to the Excel file
        source_name: Label for this source
        sheet_name:  Sheet name or index (default: first sheet)
        date_format: Optional explicit date format string

    Returns:
        Normalised DataFrame with canonical column names
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)

    if df.empty:
        raise ValueError(f"Excel file is empty: {path}")

    df = apply_full_normalisation(df, source_name, date_format)

    missing = check_required_columns(df, source_name)
    if missing:
        raise ValueError(
            f"Source '{source_name}' missing required columns after normalisation: {missing}"
        )

    return df


def load_fixed_width(
    file_path: str | Path,
    source_name: str,
    colspecs: list[tuple[int, int]],
    col_names: list[str],
    encoding: str = "utf-8",
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load a fixed-width flat file and normalise to canonical schema.

    Common in mainframe/legacy system extracts -- this is the CICS
    fixed-format record equivalent.

    Args:
        file_path:   Path to the flat file
        source_name: Label for this source
        colspecs:    List of (start, end) column position tuples
        col_names:   List of column names matching colspecs
        encoding:    File encoding
        date_format: Optional explicit date format string

    Returns:
        Normalised DataFrame with canonical column names
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Fixed-width file not found: {path}")

    df = pd.read_fwf(
        path,
        colspecs=colspecs,
        names=col_names,
        encoding=encoding,
        dtype=str,
    )

    if df.empty:
        raise ValueError(f"Fixed-width file is empty: {path}")

    df = apply_full_normalisation(df, source_name, date_format)

    missing = check_required_columns(df, source_name)
    if missing:
        raise ValueError(
            f"Source '{source_name}' missing required columns after normalisation: {missing}"
        )

    return df


def load_sqlite(
    db_path: str | Path,
    source_name: str,
    query: str,
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load trade data from a SQLite database via SQL query.

    Args:
        db_path:     Path to the SQLite database file
        source_name: Label for this source
        query:       SQL SELECT query to execute
        date_format: Optional explicit date format string

    Returns:
        Normalised DataFrame with canonical column names
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"SQLite database not found: {path}")

    conn = sqlite3.connect(path)
    try:
        df = pd.read_sql_query(query, conn, dtype=str)
    finally:
        conn.close()

    if df.empty:
        raise ValueError(f"SQLite query returned no rows for source '{source_name}'")

    df = apply_full_normalisation(df, source_name, date_format)

    missing = check_required_columns(df, source_name)
    if missing:
        raise ValueError(
            f"Source '{source_name}' missing required columns after normalisation: {missing}"
        )

    return df


# ── Generic Dispatcher ────────────────────────────────────────────────────────

def load_source(
    file_path: str | Path,
    source_name: str,
    file_type: str = "csv",
    encoding: str = "utf-8",
    date_format: Optional[str] = None,
    **kwargs,
) -> tuple[pd.DataFrame, str]:
    """
    Generic source loader -- dispatches to the right loader based on file_type.

    Returns:
        Tuple of (normalised DataFrame, file SHA256 hash)

    This is the single entry point DataIngestionAgent uses. It doesn't
    care about the source type -- it just calls load_source() and gets
    back a clean DataFrame and a hash for the audit log.
    """
    path = Path(file_path)
    file_hash = hash_file(path)

    loaders = {
        "csv":         lambda: load_csv(path, source_name, encoding, date_format),
        "excel":       lambda: load_excel(path, source_name, **kwargs),
        "xlsx":        lambda: load_excel(path, source_name, **kwargs),
        "xls":         lambda: load_excel(path, source_name, **kwargs),
        "fixed_width": lambda: load_fixed_width(path, source_name, **kwargs),
        "sqlite":      lambda: load_sqlite(path, source_name, **kwargs),
    }

    loader = loaders.get(file_type.lower())
    if loader is None:
        raise ValueError(
            f"Unsupported file type '{file_type}'. "
            f"Supported types: {list(loaders.keys())}"
        )

    df = loader()
    return df, file_hash
