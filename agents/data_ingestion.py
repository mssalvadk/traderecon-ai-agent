"""
agents/data_ingestion.py
===============================================================================
DataIngestionAgent -- loads and normalises trade data from all configured
source files into the canonical TradeRecord schema.

CICS equivalent: File Control program that READs from VSAM files and
normalises records into a standard working storage layout before passing
them to the processing program.

Responsibilities:
  1. Load source A and source B trade files
  2. Normalise to canonical column schema
  3. Validate each record against the TradeRecord pydantic model
  4. Report validation errors without stopping the pipeline
  5. Return IngestionResult for each source via PipelineContext
  6. Write audit entry for every load operation
===============================================================================
"""

import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from tools.data_loader import load_source
from tools.schemas import (
    AuditStatus,
    IngestionResult,
    PipelineContext,
    TradeRecord,
)
from tools.state_store import StateStore, new_id


AGENT_NAME = "DataIngestionAgent"


class DataIngestionAgent:
    """
    Loads trade data from configured sources and normalises to canonical schema.

    Usage:
        agent = DataIngestionAgent(store)
        context = agent.run(context, config)
    """

    def __init__(self, store: StateStore):
        self.store = store

    def run(
        self,
        context: PipelineContext,
        config: dict,
    ) -> PipelineContext:
        """
        Execute the ingestion pipeline for both sources.

        Args:
            context: Pipeline context -- updated with ingestion results
            config:  data_sources section from settings.yaml

        Returns:
            Updated PipelineContext with source_a and source_b populated
        """
        print(f"[{AGENT_NAME}] Starting data ingestion for {context.run_date}")

        # Load source A
        source_a_config = config.get("source_a", {})
        context.source_a = self._load_source(
            context=context,
            source_name="source_a",
            file_path=source_a_config.get("path", ""),
            file_type=source_a_config.get("type", "csv"),
            encoding=source_a_config.get("encoding", "utf-8"),
            date_format=source_a_config.get("date_format"),
            display_name=source_a_config.get("name", "Source A"),
        )

        # Load source B
        source_b_config = config.get("source_b", {})
        context.source_b = self._load_source(
            context=context,
            source_name="source_b",
            file_path=source_b_config.get("path", ""),
            file_type=source_b_config.get("type", "csv"),
            encoding=source_b_config.get("encoding", "utf-8"),
            date_format=source_b_config.get("date_format"),
            display_name=source_b_config.get("name", "Source B"),
        )

        total = (context.source_a.record_count if context.source_a else 0) + \
                (context.source_b.record_count if context.source_b else 0)

        print(
            f"[{AGENT_NAME}] Ingestion complete -- "
            f"Source A: {context.source_a.valid_count if context.source_a else 0} valid, "
            f"Source B: {context.source_b.valid_count if context.source_b else 0} valid"
        )

        return context

    def _load_source(
        self,
        context: PipelineContext,
        source_name: str,
        file_path: str,
        file_type: str,
        encoding: str,
        date_format: Optional[str],
        display_name: str,
    ) -> IngestionResult:
        """
        Load a single source file, validate records, return IngestionResult.

        This is the EXEC CICS READ equivalent for one source file.
        """
        start_time = time.time()

        try:
            # Load and normalise via data_loader
            df, file_hash = load_source(
                file_path=file_path,
                source_name=source_name,
                file_type=file_type,
                encoding=encoding,
                date_format=date_format,
            )

            record_count = len(df)
            valid_records, invalid_count, validation_errors = self._validate_records(df, source_name)
            valid_count = len(valid_records)

            duration_ms = int((time.time() - start_time) * 1000)

            result = IngestionResult(
                source_name=source_name,
                file_path=str(file_path),
                file_hash=file_hash,
                record_count=record_count,
                valid_count=valid_count,
                invalid_count=invalid_count,
                validation_errors=validation_errors[:10],  # Cap at 10 errors
            )

            # Write success audit entry
            self.store.quick_audit(
                agent_name=AGENT_NAME,
                action=f"load_{source_name}",
                status=AuditStatus.SUCCESS,
                run_id=context.run_id,
                detail={
                    "source_name":   source_name,
                    "display_name":  display_name,
                    "file_path":     str(file_path),
                    "record_count":  record_count,
                    "valid_count":   valid_count,
                    "invalid_count": invalid_count,
                },
                duration_ms=duration_ms,
                input_hash=file_hash,
            )

            if validation_errors:
                print(
                    f"[{AGENT_NAME}] {source_name}: {valid_count}/{record_count} valid "
                    f"({invalid_count} failed validation)"
                )
            else:
                print(f"[{AGENT_NAME}] {source_name}: {valid_count} records loaded OK")

            return result

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            error_msg = f"Failed to load {source_name}: {e}"

            # Write failure audit entry
            self.store.quick_audit(
                agent_name=AGENT_NAME,
                action=f"load_{source_name}",
                status=AuditStatus.FAILURE,
                run_id=context.run_id,
                detail={"file_path": str(file_path), "file_type": file_type},
                duration_ms=duration_ms,
                error_message=error_msg,
            )

            context.add_error(error_msg)
            print(f"[{AGENT_NAME}] ERROR: {error_msg}")

            # Return empty result so pipeline can continue in PARTIAL mode
            return IngestionResult(
                source_name=source_name,
                file_path=str(file_path),
                file_hash="",
                record_count=0,
                valid_count=0,
                invalid_count=0,
                validation_errors=[error_msg],
            )

    def _validate_records(
        self,
        df: pd.DataFrame,
        source_name: str,
    ) -> tuple[list[TradeRecord], int, list[str]]:
        """
        Validate each row in the DataFrame against the TradeRecord pydantic schema.

        Returns:
            - List of valid TradeRecord objects
            - Count of invalid records
            - List of validation error messages
        """
        valid_records: list[TradeRecord] = []
        validation_errors: list[str] = []

        for idx, row in df.iterrows():
            try:
                # Convert row to dict, handling NaN values
                row_dict = {
                    k: (None if pd.isna(v) else v)
                    for k, v in row.to_dict().items()
                    if k in TradeRecord.model_fields or k in ["source"]
                }

                record = TradeRecord(**row_dict)
                valid_records.append(record)

            except Exception as e:
                trade_id = row.get("trade_id", f"row_{idx}") if hasattr(row, "get") else f"row_{idx}"
                validation_errors.append(
                    f"{source_name}[{trade_id}]: {str(e)[:200]}"
                )

        return valid_records, len(df) - len(valid_records), validation_errors
