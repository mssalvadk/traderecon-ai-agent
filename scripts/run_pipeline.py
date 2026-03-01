"""
scripts/run_pipeline.py
═══════════════════════════════════════════════════════════════════════════════
Manual one-shot pipeline trigger for development and testing.

Bypasses the scheduler and runs the full agent pipeline immediately.
Safe to run multiple times — each run gets a unique run_id.

Usage:
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --trade-date 2024-01-15
    python scripts/run_pipeline.py --dry-run       # skips email dispatch
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the TradeRecon AI Agent pipeline manually")
    parser.add_argument(
        "--trade-date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Trade date to reconcile (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip email dispatch — print report summary to console instead.",
    )
    args = parser.parse_args()

    print()
    print("TradeRecon AI Agent — Manual Pipeline Run")
    print("=" * 60)
    print(f"  Trade Date : {args.trade_date}")
    print(f"  Dry Run    : {args.dry_run}")
    print()

    # ── OrchestratorAgent will be imported here in Phase 5 ───────────────────
    # from agents.orchestrator import OrchestratorAgent
    # orchestrator = OrchestratorAgent()
    # result = orchestrator.run(trade_date=args.trade_date, dry_run=args.dry_run)
    # print(f"\n  Pipeline completed: {result.status}")

    print("  [Phase 0] Pipeline runner scaffold ready.")
    print("  OrchestratorAgent will be wired in during Phase 5.")
    print()
    print("  For now, test individual scripts:")
    print("    python scripts/generate_sample_data.py")
    print("    python scripts/setup_db.py")
    print()


if __name__ == "__main__":
    main()
