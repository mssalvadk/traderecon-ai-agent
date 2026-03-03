"""
agents/reconciliation.py
===============================================================================
ReconciliationAgent -- orchestrates the full reconciliation pipeline,
calling the pure matching functions in tools/reconciliation_engine.py
and writing results to the pipeline context and state store.

CICS equivalent: The master reconciliation program that:
  - READs both input files
  - Calls specialist comparison sub-programs via EXEC CICS LINK
  - Writes EXCEPTION records for every break found
  - Updates the CONTROL record with summary counts
  - Issues SYNCPOINT on successful completion

This agent owns the "did we find all the breaks?" question.
The BreakAnalysisAgent (Phase 3) answers "what does each break mean?".
===============================================================================
"""

import time

import pandas as pd

from tools.reconciliation_engine import reconcile
from tools.schemas import AuditStatus, BreakSeverity, PipelineContext
from tools.state_store import StateStore


AGENT_NAME = "ReconciliationAgent"


class ReconciliationAgent:
    """
    Matches trades from two sources and identifies all break types.

    Requires DataIngestionAgent to have run first -- reads source data
    from the pipeline context, not from files directly.
    """

    def __init__(self, store: StateStore):
        self.store = store

    def run(
        self,
        context: PipelineContext,
        tolerances: dict,
        source_a_df: pd.DataFrame,
        source_b_df: pd.DataFrame,
    ) -> PipelineContext:
        """
        Run full reconciliation and populate context.breaks.

        Args:
            context:      Pipeline context -- updated with breaks
            tolerances:   Tolerance config from config/tolerance.yaml
            source_a_df:  Normalised DataFrame from DataIngestionAgent
            source_b_df:  Normalised DataFrame from DataIngestionAgent

        Returns:
            Updated PipelineContext with all breaks populated
        """
        start_time = time.time()
        print(f"[{AGENT_NAME}] Starting reconciliation for {context.run_date}")
        print(
            f"[{AGENT_NAME}] Source A: {len(source_a_df)} trades, "
            f"Source B: {len(source_b_df)} trades"
        )

        try:
            # Run the full reconciliation engine
            clean_count, breaks = reconcile(
                df_a=source_a_df,
                df_b=source_b_df,
                run_id=context.run_id,
                tolerances=tolerances,
            )

            # Populate context with results
            context.breaks = breaks

            duration_ms = int((time.time() - start_time) * 1000)

            # Count by severity for reporting
            critical = context.break_count_by_severity(BreakSeverity.CRITICAL)
            high     = context.break_count_by_severity(BreakSeverity.HIGH)
            medium   = context.break_count_by_severity(BreakSeverity.MEDIUM)
            total    = len(breaks)

            # Persist breaks to database
            if breaks:
                self.store.write_breaks(breaks)

            # Write audit entry
            self.store.quick_audit(
                agent_name=AGENT_NAME,
                action="reconciliation_complete",
                status=AuditStatus.SUCCESS,
                run_id=context.run_id,
                detail={
                    "source_a_count": len(source_a_df),
                    "source_b_count": len(source_b_df),
                    "clean_matches":  clean_count,
                    "total_breaks":   total,
                    "critical":       critical,
                    "high":           high,
                    "medium":         medium,
                    "break_types":    self._break_type_summary(breaks),
                },
                duration_ms=duration_ms,
            )

            print(f"[{AGENT_NAME}] Clean matches : {clean_count}")
            print(f"[{AGENT_NAME}] Total breaks  : {total}")
            if total > 0:
                print(f"[{AGENT_NAME}]   Critical : {critical}")
                print(f"[{AGENT_NAME}]   High     : {high}")
                print(f"[{AGENT_NAME}]   Medium   : {medium}")
                self._print_break_summary(breaks)

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            error_msg = f"Reconciliation failed: {e}"

            self.store.quick_audit(
                agent_name=AGENT_NAME,
                action="reconciliation_complete",
                status=AuditStatus.FAILURE,
                run_id=context.run_id,
                duration_ms=duration_ms,
                error_message=error_msg,
            )
            context.add_error(error_msg)
            print(f"[{AGENT_NAME}] ERROR: {error_msg}")

        return context

    def _break_type_summary(self, breaks) -> dict:
        """Count breaks by type for audit detail."""
        summary = {}
        for b in breaks:
            summary[b.break_type] = summary.get(b.break_type, 0) + 1
        return summary

    def _print_break_summary(self, breaks) -> None:
        """Print a breakdown of breaks by type to console."""
        summary = self._break_type_summary(breaks)
        for break_type, count in sorted(summary.items()):
            print(f"[{AGENT_NAME}]   {break_type:<15}: {count}")
