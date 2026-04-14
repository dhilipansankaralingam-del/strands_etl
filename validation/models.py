"""
Data models for the Strands Validation Analysis Framework.
Defines ValidationRecord (input) and AnalysisResult (output) structures.
"""

import json
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import datetime


class ValidationClassification(str, Enum):
    """Classification of a failed validation record."""
    TRUE_FAILURE = "TRUE_FAILURE"
    FALSE_POSITIVE = "FALSE_POSITIVE"
    NEEDS_INVESTIGATION = "NEEDS_INVESTIGATION"


class RecommendedAction(str, Enum):
    """Recommended actions for handling a failed validation record."""
    IGNORE = "IGNORE"
    RERUN = "RERUN"
    FIX_LOGIC = "FIX_LOGIC"
    DATA_CORRECTION = "DATA_CORRECTION"
    ESCALATE = "ESCALATE"
    MONITOR = "MONITOR"


@dataclass
class ValidationRecord:
    """
    Represents a single failed validation record for analysis.

    Populated from the failed_validations table or provided directly via CLI.
    """
    record_id: str
    rule_name: str
    table_name: str
    column_name: str
    failed_value: Any
    expected_constraint: str
    failure_timestamp: str
    run_id: str
    pipeline_name: str = ""
    database_name: str = ""
    severity: str = "MEDIUM"          # LOW | MEDIUM | HIGH | CRITICAL
    rule_type: str = "UNKNOWN"         # NOT_NULL | RANGE | REGEX | REFERENTIAL | BUSINESS
    row_count_failed: int = 1
    total_row_count: int = 0
    additional_context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def failure_rate(self) -> float:
        if self.total_row_count > 0:
            return self.row_count_failed / self.total_row_count
        return 0.0

    @classmethod
    def from_athena_row(cls, row: Dict[str, Any]) -> "ValidationRecord":
        """Construct from a raw Athena result row dict."""
        return cls(
            record_id=str(row.get("record_id", "")),
            rule_name=str(row.get("rule_name", "")),
            table_name=str(row.get("table_name", "")),
            column_name=str(row.get("column_name", "")),
            failed_value=row.get("failed_value"),
            expected_constraint=str(row.get("expected_constraint", "")),
            failure_timestamp=str(row.get("failure_timestamp", "")),
            run_id=str(row.get("run_id", "")),
            pipeline_name=str(row.get("pipeline_name", "")),
            database_name=str(row.get("database_name", "")),
            severity=str(row.get("severity", "MEDIUM")),
            rule_type=str(row.get("rule_type", "UNKNOWN")),
            row_count_failed=int(row.get("row_count_failed", 1)),
            total_row_count=int(row.get("total_row_count", 0)),
            additional_context=row.get("additional_context", {}),
        )


@dataclass
class HistoricalOutcome:
    """
    A resolved historical validation failure used to inform new analyses.
    Stored in and retrieved from S3 as learning data.
    """
    outcome_id: str
    record_id: str
    rule_name: str
    table_name: str
    column_name: str
    classification: str                # ValidationClassification value
    actual_action_taken: str           # What was done
    was_correct: Optional[bool]        # Feedback: was the classification right?
    resolution_notes: str = ""
    resolved_by: str = "system"
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    similarity_features: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AnalysisResult:
    """
    The agent's output for one ValidationRecord.

    classification: TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION
    confidence:     0.0 (uncertain) → 1.0 (very confident)
    explanation:    Human-readable reasoning
    recommended_action: one of RecommendedAction values
    root_causes:    list of probable root-cause strings
    validation_sql: optional Athena SQL to further verify the finding
    similar_historical_failures: top-k similar past outcomes
    """
    record_id: str
    classification: ValidationClassification
    confidence: float
    explanation: str
    recommended_action: RecommendedAction
    root_causes: List[str]
    suggested_next_steps: List[str] = field(default_factory=list)
    validation_sql: Optional[str] = None
    similar_historical_failures: List[Dict[str, Any]] = field(default_factory=list)
    raw_agent_response: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["classification"] = self.classification.value
        d["recommended_action"] = self.recommended_action.value
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def confidence_label(self) -> str:
        if self.confidence >= 0.85:
            return "HIGH"
        if self.confidence >= 0.60:
            return "MEDIUM"
        return "LOW"


@dataclass
class AthenaQueryResult:
    """Wraps the result of an Athena query execution."""
    query_execution_id: str
    sql: str
    nl_query: str
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: int
    data_scanned_bytes: int
    status: str           # SUCCEEDED | FAILED | CANCELLED
    error_message: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_markdown_table(self) -> str:
        """Render results as a markdown table for CLI display."""
        if not self.rows:
            return "_No rows returned._"
        header = "| " + " | ".join(self.columns) + " |"
        sep = "| " + " | ".join(["---"] * len(self.columns)) + " |"
        body_lines = []
        for row in self.rows[:50]:   # cap display at 50 rows
            cells = [str(row.get(c, "")) for c in self.columns]
            body_lines.append("| " + " | ".join(cells) + " |")
        lines = [header, sep] + body_lines
        if self.row_count > 50:
            lines.append(f"\n_... {self.row_count - 50} more rows not shown._")
        return "\n".join(lines)
