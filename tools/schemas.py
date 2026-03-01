"""
tools/schemas.py
===============================================================================
Pydantic data contract models for TradeRecon AI Agent.

These are the COMMAREA equivalent in CICS terms -- every piece of data
entering or leaving an agent must conform to one of these schemas.
If it doesn't validate, it is rejected before any processing begins.

This is the single source of truth for data shapes across all agents.
===============================================================================
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Enumerations ──────────────────────────────────────────────────────────────

class TradeSide(str, Enum):
    """Buy or Sell -- no other values permitted."""
    BUY  = "BUY"
    SELL = "SELL"


class TradeStatus(str, Enum):
    """Valid trade statuses from source systems."""
    CONFIRMED = "CONFIRMED"
    PENDING   = "PENDING"
    CANCELLED = "CANCELLED"
    FAILED    = "FAILED"


class BreakType(str, Enum):
    """All recognised break classification codes."""
    MISSING      = "MISSING"       # Trade in one source, absent in other
    DUPLICATE    = "DUPLICATE"     # Same Trade ID appears more than once
    QTY_BREAK    = "QTY_BREAK"     # Quantity / shares mismatch
    PRICE_BREAK  = "PRICE_BREAK"   # Price outside tolerance threshold
    SETTLE_BREAK = "SETTLE_BREAK"  # Settlement date mismatch
    SIDE_BREAK   = "SIDE_BREAK"    # Buy recorded as Sell or vice versa


class BreakSeverity(str, Enum):
    """Break severity levels -- drives report formatting and routing."""
    CRITICAL = "CRITICAL"
    HIGH     = "HIGH"
    MEDIUM   = "MEDIUM"
    LOW      = "LOW"


class PipelineStatus(str, Enum):
    """Pipeline run status values."""
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    PARTIAL   = "PARTIAL"   # Completed with degraded output (e.g. no AI explanations)


class AuditStatus(str, Enum):
    """Audit log entry status values."""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    WARNING = "WARNING"
    INFO    = "INFO"


# ── Core Trade Record ─────────────────────────────────────────────────────────

class TradeRecord(BaseModel):
    """
    Canonical trade record -- the normalised shape all sources are mapped to.

    This is the data contract that DataIngestionAgent produces and all
    downstream agents consume. Every field is validated on construction.
    """

    trade_id:        str        = Field(..., min_length=1, description="Unique trade identifier")
    trade_date:      date       = Field(..., description="Trade execution date")
    settlement_date: date       = Field(..., description="Settlement date (typically T+2)")
    ticker:          str        = Field(..., min_length=1, description="Exchange ticker symbol")
    isin:            str        = Field(..., description="ISIN identifier")
    side:            TradeSide  = Field(..., description="BUY or SELL")
    quantity:        float      = Field(..., gt=0, description="Number of shares/units")
    price:           float      = Field(..., gt=0, description="Execution price per share")
    consideration:   float      = Field(..., gt=0, description="Total value = quantity x price")
    counterparty:    str        = Field(..., min_length=1, description="Counterparty identifier")
    broker:          str        = Field(..., min_length=1, description="Executing broker")
    trader_id:       Optional[str] = Field(None, description="Trader identifier")
    status:          TradeStatus   = Field(TradeStatus.CONFIRMED, description="Trade status")
    source:          Optional[str] = Field(None, description="Which source file this came from")

    @field_validator("isin")
    @classmethod
    def validate_isin(cls, v: str) -> str:
        """ISIN must be 12 characters: 2 alpha country code + 10 alphanumeric."""
        v = v.strip().upper()
        if len(v) != 12:
            raise ValueError(f"ISIN must be 12 characters, got {len(v)}: '{v}'")
        if not v[:2].isalpha():
            raise ValueError(f"ISIN must start with 2 letter country code, got: '{v[:2]}'")
        return v

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("trade_id")
    @classmethod
    def validate_trade_id(cls, v: str) -> str:
        return v.strip()

    @model_validator(mode="after")
    def validate_settlement_after_trade(self) -> "TradeRecord":
        """Settlement date must be on or after trade date."""
        if self.settlement_date < self.trade_date:
            raise ValueError(
                f"Settlement date {self.settlement_date} cannot be before "
                f"trade date {self.trade_date}"
            )
        return self

    @model_validator(mode="after")
    def validate_consideration(self) -> "TradeRecord":
        """Consideration should roughly equal quantity x price (within 1%)."""
        expected = self.quantity * self.price
        tolerance = expected * 0.01  # 1% tolerance for rounding
        if abs(self.consideration - expected) > max(tolerance, 0.01):
            raise ValueError(
                f"Consideration {self.consideration} does not match "
                f"quantity x price = {expected:.4f}"
            )
        return self

    #class Config:
    #    use_enum_values = True
    model_config = {"use_enum_values": True}

# ── Break Record ──────────────────────────────────────────────────────────────

class BreakRecord(BaseModel):
    """
    A single reconciliation break found between two sources.
    Produced by ReconciliationAgent, enriched by BreakAnalysisAgent.
    """

    break_id:        str          = Field(..., description="Unique break identifier (UUID)")
    run_id:          str          = Field(..., description="Pipeline run this break belongs to")
    trade_id:        Optional[str]= Field(None, description="Trade ID (None for MISSING breaks)")
    break_type:      BreakType    = Field(..., description="Classification of the break")
    severity:        BreakSeverity= Field(..., description="Severity level")
    source_a_value:  Optional[str]= Field(None, description="JSON value from source A")
    source_b_value:  Optional[str]= Field(None, description="JSON value from source B")
    difference:      Optional[str]= Field(None, description="Calculated difference")
    ai_explanation:  Optional[str]= Field(None, description="Claude AI plain-English explanation")
    ai_generated_at: Optional[datetime] = Field(None, description="When AI explanation was generated")
    resolved:        bool         = Field(False, description="Whether break has been resolved")

    #class Config:
    #    use_enum_values = True
    model_config = {"use_enum_values": True}


# ── Pipeline Run ──────────────────────────────────────────────────────────────

class PipelineRun(BaseModel):
    """
    Records a single execution of the full pipeline.
    Equivalent to a CICS task entry in the monitoring records.
    """

    run_id:          str            = Field(..., description="UUID for this run")
    run_date:        date           = Field(..., description="Trade date being reconciled")
    started_at:      datetime       = Field(..., description="Pipeline start time")
    completed_at:    Optional[datetime] = Field(None, description="Completion time")
    status:          PipelineStatus = Field(PipelineStatus.RUNNING)
    total_breaks:    int            = Field(0, ge=0)
    critical_breaks: int            = Field(0, ge=0)
    high_breaks:     int            = Field(0, ge=0)
    medium_breaks:   int            = Field(0, ge=0)
    source_a_count:  int            = Field(0, ge=0)
    source_b_count:  int            = Field(0, ge=0)
    matched_count:   int            = Field(0, ge=0)
    report_path:     Optional[str]  = Field(None)
    email_sent:      bool           = Field(False)
    error_message:   Optional[str]  = Field(None)

    # class Config:
    #     use_enum_values = True
    model_config = {"use_enum_values": True}

# ── Audit Log Entry ───────────────────────────────────────────────────────────

class AuditEntry(BaseModel):
    """
    A single audit trail entry written by AuditAgent.
    Equivalent to a CICS SMF 110 record -- every significant event is captured.
    """

    audit_id:      str               = Field(..., description="UUID for this entry")
    run_id:        Optional[str]     = Field(None, description="Pipeline run (None for startup)")
    agent_name:    str               = Field(..., description="Which agent generated this")
    action:        str               = Field(..., description="What action was taken")
    status:        AuditStatus       = Field(..., description="Outcome of the action")
    detail:        Optional[dict]    = Field(None, description="Structured detail payload")
    duration_ms:   Optional[int]     = Field(None, ge=0, description="Duration in milliseconds")
    input_hash:    Optional[str]     = Field(None, description="SHA256 hash of input data")
    error_message: Optional[str]     = Field(None, description="Error details if status=FAILURE")
    created_at:    datetime          = Field(default_factory=lambda: datetime.now(timezone.utc))

    # class Config:
    #     use_enum_values = True
    model_config = {"use_enum_values": True}

# ── Ingestion Result ──────────────────────────────────────────────────────────

class IngestionResult(BaseModel):
    """
    Result returned by DataIngestionAgent after loading a source file.
    Passed via pipeline context to ReconciliationAgent.
    """

    source_name:    str              = Field(..., description="Identifier for this source")
    file_path:      str              = Field(..., description="Path to the source file")
    file_hash:      str              = Field(..., description="SHA256 hash of source file")
    record_count:   int              = Field(0, ge=0, description="Number of records loaded")
    valid_count:    int              = Field(0, ge=0, description="Records that passed validation")
    invalid_count:  int              = Field(0, ge=0, description="Records that failed validation")
    validation_errors: list[str]     = Field(default_factory=list)
    loaded_at:      datetime         = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Pipeline Context ──────────────────────────────────────────────────────────

class PipelineContext(BaseModel):
    """
    The shared context object passed between all agents in the pipeline.

    This is the COMMAREA of the entire pipeline -- the explicit state
    contract that carries data from one agent to the next.

    OrchestratorAgent creates it. Each agent reads from and writes to it.
    Nothing is stored in agent memory -- all state lives here.
    """

    run_id:         str                      = Field(..., description="UUID for this pipeline run")
    run_date:       date                     = Field(..., description="Trade date being processed")
    dry_run:        bool                     = Field(False, description="If True skip email send")
    source_a:       Optional[IngestionResult]= Field(None)
    source_b:       Optional[IngestionResult]= Field(None)
    breaks:         list[BreakRecord]        = Field(default_factory=list)
    pipeline_run:   Optional[PipelineRun]    = Field(None)
    report_path:    Optional[str]            = Field(None)
    email_sent:     bool                     = Field(False)
    errors:         list[str]                = Field(default_factory=list)

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def break_count_by_severity(self, severity: str) -> int:
        return sum(1 for b in self.breaks if b.severity == severity)

    # class Config:
    #     use_enum_values = True
    model_config = {"use_enum_values": True}
