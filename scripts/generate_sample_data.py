"""
scripts/generate_sample_data.py
═══════════════════════════════════════════════════════════════════════════════
Generates realistic sample equities trade data for development and testing.

Creates two CSV files simulating:
  - Source A: Internal trade blotter (what OUR system recorded)
  - Source B: Counterparty / custodian feed (what THEY recorded)

Intentionally introduces breaks of all 6 types so the reconciliation
engine has real data to work with.

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --trade-date 2024-01-15
    python scripts/generate_sample_data.py --num-trades 100
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


# ── Constants ─────────────────────────────────────────────────────────────────

EQUITIES = [
    ("AAPL",  "US0378331005", "Apple Inc"),
    ("MSFT",  "US5949181045", "Microsoft Corporation"),
    ("GOOGL", "US02079K3059", "Alphabet Inc Class A"),
    ("AMZN",  "US0231351067", "Amazon.com Inc"),
    ("NVDA",  "US67066G1040", "NVIDIA Corporation"),
    ("JPM",   "US46625H1005", "JPMorgan Chase & Co"),
    ("GS",    "US38141G1040", "Goldman Sachs Group Inc"),
    ("BAC",   "US0605051046", "Bank of America Corp"),
    ("MS",    "US6174464486", "Morgan Stanley"),
    ("BLK",   "US09248X1000", "BlackRock Inc"),
]

COUNTERPARTIES = [
    "CITI_SECURITIES",
    "BARCLAYS_CAP",
    "DEUTSCHE_BANK",
    "UBS_SECURITIES",
    "CREDIT_SUISSE",
]

BROKERS = [
    "INSTINET",
    "LIQUIDNET",
    "ITG_POSIT",
    "GOLDMAN_ALGO",
    "MORGAN_ALGO",
]

TRADE_FIELDS = [
    "trade_id",
    "trade_date",
    "settlement_date",
    "ticker",
    "isin",
    "side",
    "quantity",
    "price",
    "consideration",
    "counterparty",
    "broker",
    "trader_id",
    "status",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def settlement_date(trade_date: datetime, offset_days: int = 2) -> str:
    """T+2 settlement — skip weekends."""
    sd = trade_date
    added = 0
    while added < offset_days:
        sd += timedelta(days=1)
        if sd.weekday() < 5:  # Mon–Fri only
            added += 1
    return sd.strftime("%Y-%m-%d")


def make_trade(
    trade_id: str,
    trade_date: datetime,
    ticker: str,
    isin: str,
    side: str,
    quantity: int,
    price: float,
    counterparty: str,
    broker: str,
    settle_offset: int = 2,
) -> dict:
    """Build a single trade record dict."""
    consideration = round(quantity * price, 2)
    return {
        "trade_id":        trade_id,
        "trade_date":      trade_date.strftime("%Y-%m-%d"),
        "settlement_date": settlement_date(trade_date, settle_offset),
        "ticker":          ticker,
        "isin":            isin,
        "side":            side,
        "quantity":        quantity,
        "price":           price,
        "consideration":   consideration,
        "counterparty":    counterparty,
        "broker":          broker,
        "trader_id":       f"TDR{random.randint(100, 999)}",
        "status":          "CONFIRMED",
    }


def write_csv(path: Path, trades: list[dict]) -> None:
    """Write list of trade dicts to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_FIELDS)
        writer.writeheader()
        writer.writerows(trades)
    print(f"  ✓ Written {len(trades)} trades → {path}")


# ── Main Generator ────────────────────────────────────────────────────────────

def generate(trade_date: datetime, num_trades: int = 50) -> None:
    """
    Generate source_a and source_b trade files with intentional breaks.

    Break distribution:
      - 70% clean matched trades
      - 8%  MISSING (in A only — source B never received it)
      - 5%  DUPLICATE (extra entry in source B)
      - 5%  QTY_BREAK (quantity differs)
      - 5%  PRICE_BREAK (price outside tolerance)
      - 4%  SETTLE_BREAK (settlement date differs)
      - 3%  SIDE_BREAK (buy/sell flipped — most critical)
    """
    random.seed(42)  # Reproducible output for testing

    source_a: list[dict] = []
    source_b: list[dict] = []

    # ── CLEAN TRADES (70%) ────────────────────────────────────────────────────
    clean_count = int(num_trades * 0.70)
    for i in range(clean_count):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        side = random.choice(["BUY", "SELL"])
        qty = random.randint(100, 10000)
        price = round(random.uniform(10.0, 500.0), 4)
        cp = random.choice(COUNTERPARTIES)
        broker = random.choice(BROKERS)
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{i:04d}"

        trade = make_trade(tid, trade_date, ticker, isin, side, qty, price, cp, broker)
        source_a.append(trade)
        source_b.append(trade.copy())  # Exact copy → clean match

    idx = clean_count

    # ── MISSING TRADES in B (8%) ──────────────────────────────────────────────
    for i in range(int(num_trades * 0.08)):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{idx:04d}"
        trade = make_trade(
            tid, trade_date, ticker, isin,
            random.choice(["BUY", "SELL"]),
            random.randint(100, 5000),
            round(random.uniform(10.0, 500.0), 4),
            random.choice(COUNTERPARTIES),
            random.choice(BROKERS),
        )
        source_a.append(trade)
        # NOT added to source_b → MISSING break
        idx += 1

    # ── DUPLICATE in B (5%) ───────────────────────────────────────────────────
    for i in range(int(num_trades * 0.05)):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{idx:04d}"
        trade = make_trade(
            tid, trade_date, ticker, isin,
            random.choice(["BUY", "SELL"]),
            random.randint(100, 5000),
            round(random.uniform(10.0, 500.0), 4),
            random.choice(COUNTERPARTIES),
            random.choice(BROKERS),
        )
        source_a.append(trade)
        source_b.append(trade.copy())
        source_b.append(trade.copy())  # Duplicate entry in B
        idx += 1

    # ── QUANTITY BREAK (5%) ───────────────────────────────────────────────────
    for i in range(int(num_trades * 0.05)):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{idx:04d}"
        qty_a = random.randint(100, 5000)
        qty_b = qty_a + random.choice([-50, -100, 50, 100, 200])  # Different qty
        price = round(random.uniform(10.0, 500.0), 4)
        side = random.choice(["BUY", "SELL"])
        cp = random.choice(COUNTERPARTIES)
        broker = random.choice(BROKERS)

        trade_a = make_trade(tid, trade_date, ticker, isin, side, qty_a, price, cp, broker)
        trade_b = make_trade(tid, trade_date, ticker, isin, side, qty_b, price, cp, broker)
        source_a.append(trade_a)
        source_b.append(trade_b)
        idx += 1

    # ── PRICE BREAK (5%) ──────────────────────────────────────────────────────
    for i in range(int(num_trades * 0.05)):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{idx:04d}"
        price_a = round(random.uniform(10.0, 500.0), 4)
        price_b = round(price_a + random.choice([0.05, 0.10, -0.05, 0.25, -0.10]), 4)
        qty = random.randint(100, 5000)
        side = random.choice(["BUY", "SELL"])
        cp = random.choice(COUNTERPARTIES)
        broker = random.choice(BROKERS)

        trade_a = make_trade(tid, trade_date, ticker, isin, side, qty, price_a, cp, broker)
        trade_b = make_trade(tid, trade_date, ticker, isin, side, qty, price_b, cp, broker)
        source_a.append(trade_a)
        source_b.append(trade_b)
        idx += 1

    # ── SETTLEMENT DATE BREAK (4%) ────────────────────────────────────────────
    for i in range(int(num_trades * 0.04)):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{idx:04d}"
        qty = random.randint(100, 5000)
        price = round(random.uniform(10.0, 500.0), 4)
        side = random.choice(["BUY", "SELL"])
        cp = random.choice(COUNTERPARTIES)
        broker = random.choice(BROKERS)

        trade_a = make_trade(tid, trade_date, ticker, isin, side, qty, price, cp, broker, settle_offset=2)
        trade_b = make_trade(tid, trade_date, ticker, isin, side, qty, price, cp, broker, settle_offset=3)
        source_a.append(trade_a)
        source_b.append(trade_b)
        idx += 1

    # ── SIDE BREAK (3%) — most critical ──────────────────────────────────────
    for i in range(int(num_trades * 0.03)):
        equity = random.choice(EQUITIES)
        ticker, isin, _ = equity
        tid = f"TRD{trade_date.strftime('%Y%m%d')}{idx:04d}"
        qty = random.randint(100, 5000)
        price = round(random.uniform(10.0, 500.0), 4)
        cp = random.choice(COUNTERPARTIES)
        broker = random.choice(BROKERS)

        trade_a = make_trade(tid, trade_date, ticker, isin, "BUY",  qty, price, cp, broker)
        trade_b = make_trade(tid, trade_date, ticker, isin, "SELL", qty, price, cp, broker)
        source_a.append(trade_a)
        source_b.append(trade_b)
        idx += 1

    # ── Shuffle to simulate real-world unsorted feeds ─────────────────────────
    random.shuffle(source_a)
    random.shuffle(source_b)

    # ── Write output ──────────────────────────────────────────────────────────
    base = Path("data/samples")
    write_csv(base / "source_a_trades.csv", source_a)
    write_csv(base / "source_b_trades.csv", source_b)

    # ── Reference data (security master) ─────────────────────────────────────
    ref_data = [
        {"ticker": e[0], "isin": e[1], "name": e[2], "asset_class": "EQUITY", "currency": "USD"}
        for e in EQUITIES
    ]
    ref_path = base / "reference_data.csv"
    with open(ref_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "isin", "name", "asset_class", "currency"])
        writer.writeheader()
        writer.writerows(ref_data)
    print(f"  ✓ Written {len(ref_data)} securities → {ref_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("═" * 60)
    print(f"  Trade Date  : {trade_date.strftime('%Y-%m-%d')}")
    print(f"  Source A    : {len(source_a)} trades")
    print(f"  Source B    : {len(source_b)} trades")
    print(f"  Expected    : ~{int(num_trades * 0.30)} breaks across 6 break types")
    print("═" * 60)
    print()
    print("  Next step:")
    print("    python scripts/setup_db.py")
    print()


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample trade data for TradeRecon AI Agent")
    parser.add_argument(
        "--trade-date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Trade date to use (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--num-trades",
        type=int,
        default=50,
        help="Approximate number of trades to generate. Defaults to 50.",
    )
    args = parser.parse_args()

    try:
        trade_date = datetime.strptime(args.trade_date, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: Invalid date format '{args.trade_date}'. Use YYYY-MM-DD.")
        raise SystemExit(1)

    print()
    print("TradeRecon AI Agent — Sample Data Generator")
    print("=" * 60)
    print()
    generate(trade_date, args.num_trades)
