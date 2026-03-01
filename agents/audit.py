"""
agents/audit.py
===============================================================================
AuditAgent -- writes the final structured audit trail entry after every
pipeline run, regardless of success or failure.

CICS equivalent: SMF Type 110 records -- every significant transaction
event is captured in a structured, queryable audit log.

The audit trail is append-only. It records:
  - Which agents ran and in what order
  - How long each step took
  - What data was processed (via hashes, not raw data)
  - Any errors that occurred
  - Final pipeline status

This gives operations staff a complete, tamper-evident record of every
reconciliation run -- essential for financial services compliance.
===============================================================================
"""

import time
from datetime import datetime

from tools.schemas import AuditStatus, PipelineContext, PipelineStatus
from tools.state_store import StateStore


AGENT_NAME = "AuditAgent"


class AuditAgent:
    """
    Writes the final audit trail entry and completes the pipeline run record.

    Called last in every pipeline execution -- success or failure.
    Never raises exceptions -- audit writing must always succeed.
    """

    def __init__(self, store: StateStore):
        self.store = store

    def run(
        self,
        context: PipelineContext,
        pipeline_start_time: float,
    ) -> PipelineContext:
        """
        Write final audit entry and complete the pipeline_runs record.

        Args:
            context:             Final pipeline context
            pipeline_start_time: time.time() from pipeline start (for duration)

        Returns:
            Unchanged context (audit agent does not modify pipeline data)
        """
        try:
            duration_ms = int((time.time() - pipeline_start_time) * 1000)

            # Determine final status
            if context.has_errors() and not context.breaks:
                status = PipelineStatus.FAILED
            elif context.has_errors():
                status = PipelineStatus.PARTIAL  # Ran with some errors but produced output
            else:
                status = PipelineStatus.COMPLETED

            # Count breaks by severity
            critical = context.break_count_by_severity("CRITICAL")
            high     = context.break_count_by_severity("HIGH")
            medium   = context.break_count_by_severity("MEDIUM")
            total    = len(context.breaks)

            # Complete the pipeline run record
            self.store.complete_run(
                run_id=context.run_id,
                status=status,
                total_breaks=total,
                critical_breaks=critical,
                high_breaks=high,
                medium_breaks=medium,
                source_a_count=context.source_a.valid_count if context.source_a else 0,
                source_b_count=context.source_b.valid_count if context.source_b else 0,
                report_path=context.report_path,
                email_sent=context.email_sent,
                error_message="; ".join(context.errors) if context.errors else None,
            )

            # Write final summary audit entry
            self.store.quick_audit(
                agent_name=AGENT_NAME,
                action="pipeline_complete",
                status=AuditStatus.SUCCESS if status == PipelineStatus.COMPLETED else AuditStatus.WARNING,
                run_id=context.run_id,
                detail={
                    "final_status":   str(status),
                    "total_breaks":   total,
                    "critical":       critical,
                    "high":           high,
                    "medium":         medium,
                    "source_a_count": context.source_a.valid_count if context.source_a else 0,
                    "source_b_count": context.source_b.valid_count if context.source_b else 0,
                    "report_path":    context.report_path,
                    "email_sent":     context.email_sent,
                    "error_count":    len(context.errors),
                    "dry_run":        context.dry_run,
                },
                duration_ms=duration_ms,
            )

            print(f"[{AGENT_NAME}] Pipeline run {context.run_id[:8]}... -> {status}")
            print(f"[{AGENT_NAME}] Breaks: {total} total ({critical} critical, {high} high, {medium} medium)")
            print(f"[{AGENT_NAME}] Duration: {duration_ms}ms")

        except Exception as e:
            # Audit writing must never crash the pipeline
            # If it fails, print to console at minimum
            print(f"[{AGENT_NAME}] WARNING: Failed to write audit trail: {e}")

        return context
